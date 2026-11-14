use super::*;
use crate::{
    driver::{
        tasks::error::{Error, Result},
        Channels,
        DecodeConfig,
        DecodeMode,
    },
    events::context_data::{RtpData, VoiceData},
};
use discortp::{rtp::RtpExtensionPacket, Packet, PacketSize};
use opus2::{Decoder as OpusDecoder, ErrorCode};
use tracing::{error, warn};

#[derive(Debug)]
pub struct SsrcState {
    playout_buffer: PlayoutBuffer,
    crypto_mode: CryptoMode,
    decoder: OpusDecoder,
    decode_size: PacketDecodeSize,
    pub(crate) prune_time: Instant,
    pub(crate) disconnected: bool,
    channels: Channels,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use discortp::rtp::{MutableRtpPacket, RtpType};
    use opus2::{Application, Channels, Encoder};
    use std::num::NonZeroU8;

    fn check_payload_bounds(extension_words: Option<u16>, dave_overhead: usize) {
        // Force CELT, whose end-of-frame data is corrupted by stale DAVE bytes.
        let mut encoder = Encoder::new(48000, Channels::Stereo, Application::LowDelay).unwrap();
        let tone: Vec<f32> = (0..960)
            .flat_map(|i| {
                let sample = (std::f32::consts::TAU * 440.0 * i as f32 / 48000.0).sin() * 0.2;
                [sample, sample]
            })
            .collect();
        let mut opus = vec![0; 4000];
        let len = encoder.encode_float(&tone, &mut opus).unwrap();
        opus.truncate(len);
        let mut expected = vec![0_i16; 1920];
        OpusDecoder::new(48000, Channels::Stereo)
            .unwrap()
            .decode(&opus, &mut expected, false)
            .unwrap();

        let crypto_mode = CryptoMode::Aes256Gcm;
        let trailer = crypto_mode.payload_suffix_len() + dave_overhead;
        let extension_len = extension_words.map_or(0, |words| 4 + usize::from(words) * 4);
        let mut bytes = vec![0_u8; 12 + extension_len + opus.len() + trailer];
        let mut rtp = MutableRtpPacket::new(&mut bytes).unwrap();
        rtp.set_version(2);
        rtp.set_extension(u8::from(extension_words.is_some()));
        rtp.set_payload_type(RtpType::Dynamic(120));
        rtp.set_sequence(1.into());
        rtp.set_timestamp(960.into());
        rtp.set_ssrc(7);
        let payload = rtp.payload_mut();
        if let Some(words) = extension_words {
            payload[..2].copy_from_slice(&0xBEDE_u16.to_be_bytes());
            payload[2..4].copy_from_slice(&words.to_be_bytes());
        }
        payload[extension_len..extension_len + opus.len()].copy_from_slice(&opus);
        // In-place decryption overwrites the Opus frame but leaves the rest of
        // the old DAVE envelope and transport trailer in the packet allocation.
        payload[extension_len + opus.len()..].fill(0xAA);

        let config = Config::default()
            .decode_mode(DecodeMode::Decode(Default::default()))
            .playout_buffer_length(NonZeroU8::new(1).unwrap());
        let mut state = SsrcState::new(&rtp.to_immutable(), crypto_mode, &config);
        state.store_packet(
            StoredPacket {
                packet: Bytes::from(bytes),
                decrypted: true,
                payload_end_pad: trailer,
            },
            &config,
        );
        let decoded = state.get_voice_tick(&config).unwrap().unwrap();
        assert_eq!(
            decoded.decoded_voice.unwrap(),
            expected,
            "extension_words={extension_words:?}, dave_overhead={dave_overhead}"
        );
        assert_eq!(decoded.packet.unwrap().payload_end_pad, trailer);
    }

    #[test]
    fn dave_payload_bounds_survive_playout() {
        for extension_words in [None, Some(0), Some(1), Some(4)] {
            for dave_overhead in [11, 17, 37] {
                check_payload_bounds(extension_words, dave_overhead);
            }
        }
    }

    #[test]
    fn transport_only_payload_bounds_survive_playout() {
        for extension_words in [None, Some(0), Some(1), Some(4)] {
            check_payload_bounds(extension_words, 0);
        }
    }
}

impl SsrcState {
    pub fn new(pkt: &RtpPacket<'_>, crypto_mode: CryptoMode, config: &Config) -> Self {
        let playout_capacity = config.playout_buffer_length.get() + config.playout_spike_length;
        let (sample_rate, channels) = match config.decode_mode {
            DecodeMode::Decode(config) => (config.sample_rate, config.channels),
            DecodeMode::Decrypt | DecodeMode::Pass => Default::default(),
        };

        Self {
            playout_buffer: PlayoutBuffer::new(usize::from(playout_capacity), pkt.get_sequence().0),
            crypto_mode,
            decoder: OpusDecoder::new(sample_rate.into(), channels.into())
                .expect("Failed to create new Opus decoder for source."),
            decode_size: PacketDecodeSize::TwentyMillis,
            prune_time: Instant::now() + config.decode_state_timeout.into(),
            disconnected: false,
            channels,
        }
    }

    pub fn reconfigure_decoder(&mut self, config: DecodeConfig) {
        self.decoder = OpusDecoder::new(config.sample_rate.into(), config.channels.into())
            .expect("Failed to create new Opus decoder for source.");

        self.channels = config.channels;
    }

    pub fn store_packet(&mut self, packet: StoredPacket, config: &Config) {
        self.playout_buffer.store_packet(packet, config);
    }

    pub fn refresh_timer(&mut self, state_timeout: Duration) {
        if !self.disconnected {
            self.prune_time = Instant::now() + state_timeout;
        }
    }

    pub fn get_voice_tick(&mut self, config: &Config) -> Result<Option<VoiceData>> {
        // Acquire a packet from the playout buffer:
        // Update nexts, lasts...
        // different cases: null packet who we want to decode as a miss, and packet who we must ignore temporarily.
        let m_pkt = self.playout_buffer.fetch_packet(config);
        let pkt = match m_pkt {
            PacketLookup::Packet(packet) => Some(packet),
            PacketLookup::MissedPacket => None,
            PacketLookup::Filling => return Ok(None),
        };

        let mut out = VoiceData {
            packet: None,
            decoded_voice: None,
        };

        let should_decode = config.decode_mode.should_decode();
        if let Some(StoredPacket {
            packet,
            decrypted,
            payload_end_pad,
        }) = pkt
        {
            let rtp = RtpPacket::new(&packet).unwrap();
            let extensions = rtp.get_extension() != 0;

            let payload = rtp.payload();
            let payload_offset = self.crypto_mode.payload_prefix_len();
            let payload_end = payload.len() - payload_end_pad;

            // We still need to compute missed packets here in case of long loss chains or similar.
            // This occurs due to the fallback in 'store_packet' (i.e., empty buffer and massive seq difference).
            // Normal losses should be handled by the below `else` branch.
            let new_seq: u16 = rtp.get_sequence().into();
            let missed_packets = new_seq.saturating_sub(self.playout_buffer.next_seq().0);

            // TODO: maybe hand over audio and extension indices alongside packet?
            let (audio, _packet_size) = self.scan_and_decode(
                &payload[payload_offset..payload_end],
                extensions,
                missed_packets,
                should_decode && decrypted,
            )?;

            let rtp_data = RtpData {
                packet,
                payload_offset,
                payload_end_pad,
            };

            out.packet = Some(rtp_data);
            out.decoded_voice = audio;
        } else if should_decode {
            let mut audio = vec![0; self.decode_size.len()];
            let len = self.decoder.decode(&[], &mut audio[..], false)?;
            audio.truncate(2 * len);

            out.decoded_voice = Some(audio);
        }

        Ok(Some(out))
    }

    fn scan_and_decode(
        &mut self,
        data: &[u8],
        extension: bool,
        missed_packets: u16,
        decode: bool,
    ) -> Result<(Option<Vec<i16>>, usize)> {
        let start = if extension {
            RtpExtensionPacket::new(data)
                .map(|pkt| pkt.packet_size())
                .ok_or_else(|| {
                    error!("Extension packet indicated, but insufficient space.");
                    Error::IllegalVoicePacket
                })
        } else {
            Ok(0)
        }?;

        let pkt = if decode {
            let mut out = vec![0; self.decode_size.len()];

            for _ in 0..missed_packets {
                if let Err(e) = self.decoder.decode(&[], &mut out[..], false) {
                    warn!("Issue while decoding for missed packet: {:?}.", e);
                }
            }

            // In general, we should expect 20 ms frames.
            // However, Discord occasionally like to surprise us with something bigger.
            // This is *sender-dependent behaviour*.
            //
            // This should scan up to find the "correct" size that a source is using,
            // and then remember that.
            loop {
                let tried_audio_len = self.decoder.decode(&data[start..], &mut out, false);
                match tried_audio_len {
                    Ok(audio_len) => {
                        // Decoding to stereo: audio_len refers to sample count irrespective of channel count.
                        // => multiply by number of channels.
                        out.truncate(self.channels.channels() * audio_len);

                        break;
                    },
                    Err(e) if e.code() == ErrorCode::BufferTooSmall => {
                        if self.decode_size.can_bump_up() {
                            self.decode_size = self.decode_size.bump_up();
                            out = vec![0; self.decode_size.len()];
                        } else {
                            error!("Received packet larger than Opus standard maximum,");
                            return Err(Error::IllegalVoicePacket);
                        }
                    },
                    Err(e) => {
                        error!("Failed to decode received packet: {:?}.", e);
                        return Err(e.into());
                    },
                }
            }

            Some(out)
        } else {
            None
        };

        Ok((pkt, data.len() - start))
    }
}

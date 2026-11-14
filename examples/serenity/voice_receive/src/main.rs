//! Requires the "gateway", "cache", and "voice" features be enabled
//! in your Cargo.toml, like so:
//!
//! ```toml
//! [dependencies.serenity]
//! git = "https://github.com/serenity-rs/serenity.git"
//! branch = "next"
//! features = ["gateway", "cache", "voice"]
//! ```
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use dashmap::DashMap;

use serenity::all::{FullEvent, Token};
use serenity::{
    async_trait,
    gateway::client::{Client, Context, EventHandler},
    model::{channel::Message, id::ChannelId},
    prelude::{GatewayIntents, Mentionable},
    Result as SerenityResult,
};

use songbird::{
    driver::{DecodeConfig, DecodeMode},
    model::{
        id::UserId,
        payload::{ClientDisconnect, Speaking},
    },
    packet::Packet,
    Config,
    CoreEvent,
    Event,
    EventContext,
    EventHandler as VoiceEventHandler,
    Songbird,
};

struct UserData {
    songbird: Arc<Songbird>,
}

struct Handler;

type CommandResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
impl EventHandler for Handler {
    async fn dispatch(&self, ctx: &Context, event: &FullEvent) {
        match event {
            FullEvent::Ready { data_about_bot, .. } => {
                println!("{} is connected!", data_about_bot.user.name);
            },
            FullEvent::Message {
                new_message: msg, ..
            } => {
                if msg.author.bot() {
                    return;
                }
                let Some(content) = msg.content.strip_prefix('~') else {
                    return;
                };
                let (command, args) = content
                    .split_once(char::is_whitespace)
                    .unwrap_or((content, ""));
                if msg.guild_id.is_none() && command != "ping" {
                    return;
                }
                let result = match command {
                    "join" => join(ctx, msg, args).await,
                    "leave" => leave(ctx, msg).await,
                    "ping" => ping(ctx, msg).await,
                    _ => return,
                };
                if let Err(error) = result {
                    eprintln!("Command {command} failed: {error}");
                }
            },
            _ => {},
        }
    }
}

#[derive(Clone)]
struct Receiver {
    inner: Arc<InnerReceiver>,
}

struct InnerReceiver {
    last_tick_was_empty: AtomicBool,
    known_ssrcs: DashMap<u32, UserId>,
}

impl Receiver {
    pub fn new() -> Self {
        // You can manage state here, such as a buffer of audio packet bytes so
        // you can later store them in intervals.
        Self {
            inner: Arc::new(InnerReceiver {
                last_tick_was_empty: AtomicBool::default(),
                known_ssrcs: DashMap::new(),
            }),
        }
    }
}

#[async_trait]
impl VoiceEventHandler for Receiver {
    #[allow(unused_variables)]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        use EventContext as Ctx;
        match ctx {
            Ctx::SpeakingStateUpdate(Speaking {
                speaking,
                ssrc,
                user_id,
                ..
            }) => {
                // Discord voice calls use RTP, where every sender uses a randomly allocated
                // *Synchronisation Source* (SSRC) to allow receivers to tell which audio
                // stream a received packet belongs to. As this number is not derived from
                // the sender's user_id, only Discord Voice Gateway messages like this one
                // inform us about which random SSRC a user has been allocated. Future voice
                // packets will contain *only* the SSRC.
                //
                // You can implement logic here so that you can differentiate users'
                // SSRCs and map the SSRC to the User ID and maintain this state.
                // Using this map, you can map the `ssrc` in `voice_packet`
                // to the user ID and handle their audio packets separately.
                println!(
                    "Speaking state update: user {:?} has SSRC {:?}, using {:?}",
                    user_id, ssrc, speaking,
                );

                if let Some(user) = user_id {
                    self.inner.known_ssrcs.insert(*ssrc, *user);
                }
            },
            Ctx::VoiceTick(tick) => {
                let speaking = tick.speaking.len();
                let total_participants = speaking + tick.silent.len();
                let last_tick_was_empty = self.inner.last_tick_was_empty.load(Ordering::SeqCst);

                if speaking == 0 && !last_tick_was_empty {
                    println!("No speakers");

                    self.inner.last_tick_was_empty.store(true, Ordering::SeqCst);
                } else if speaking != 0 {
                    self.inner
                        .last_tick_was_empty
                        .store(false, Ordering::SeqCst);

                    println!("Voice tick ({speaking}/{total_participants} live):");

                    // You can also examine tick.silent to see users who are present
                    // but haven't spoken in this tick.
                    for (ssrc, data) in &tick.speaking {
                        let user_id_str = if let Some(id) = self.inner.known_ssrcs.get(ssrc) {
                            format!("{:?}", *id)
                        } else {
                            "?".into()
                        };

                        // This field should *always* exist under DecodeMode::Decode.
                        // The `else` allows you to see how the other modes are affected.
                        if let Some(decoded_voice) = data.decoded_voice.as_ref() {
                            let voice_len = decoded_voice.len();
                            let audio_str = format!(
                                "first samples from {}: {:?}",
                                voice_len,
                                &decoded_voice[..voice_len.min(5)]
                            );

                            if let Some(packet) = &data.packet {
                                let rtp = packet.rtp();
                                println!(
                                    "\t{ssrc}/{user_id_str}: packet seq {} ts {} -- {audio_str}",
                                    rtp.get_sequence().0,
                                    rtp.get_timestamp().0
                                );
                            } else {
                                println!("\t{ssrc}/{user_id_str}: Missed packet -- {audio_str}");
                            }
                        } else {
                            println!("\t{ssrc}/{user_id_str}: Decode disabled.");
                        }
                    }
                }
            },
            Ctx::RtpPacket(packet) => {
                // An event which fires for every received audio packet,
                // containing the decoded data.
                let rtp = packet.rtp();
                println!(
                    "Received voice packet from SSRC {}, sequence {}, timestamp {} -- {}B long",
                    rtp.get_ssrc(),
                    rtp.get_sequence().0,
                    rtp.get_timestamp().0,
                    rtp.payload().len()
                );
            },
            Ctx::RtcpPacket(data) => {
                // An event which fires for every received rtcp packet,
                // containing the call statistics and reporting information.
                println!("RTCP packet received: {:?}", data.packet);
            },
            Ctx::ClientDisconnect(ClientDisconnect { user_id, .. }) => {
                // You can implement your own logic here to handle a user who has left the
                // voice channel e.g., finalise processing of statistics etc.
                // You will typically need to map the User ID to their SSRC; observed when
                // first speaking.

                println!("Client disconnected: user {:?}", user_id);
            },
            _ => {
                // We won't be registering this struct for any more event classes.
                unimplemented!()
            },
        }

        None
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Configure the client with your Discord bot token in the environment.
    let token =
        Token::from_env("DISCORD_TOKEN").expect("Expected a valid token in the environment");

    let intents = GatewayIntents::non_privileged() | GatewayIntents::MESSAGE_CONTENT;

    // Here, we need to configure Songbird to decode all incoming voice packets.
    // If you want, you can do this on a per-call basis---here, we need it to
    // read the audio data that other people are sending us!
    let songbird_config =
        Config::default().decode_mode(DecodeMode::Decode(DecodeConfig::default()));
    let manager = songbird::Songbird::serenity_from_config(songbird_config);

    let data = UserData {
        songbird: Arc::clone(&manager),
    };

    let mut client = Client::builder(token, intents)
        .voice_manager(manager)
        .data(Arc::new(data))
        .event_handler(Arc::new(Handler))
        .await
        .expect("Err creating client");

    let _ = client
        .start()
        .await
        .map_err(|why| println!("Client ended: {:?}", why));
}

async fn join(ctx: &Context, msg: &Message, args: &str) -> CommandResult {
    let Ok(connect_to) = args.trim().parse::<ChannelId>() else {
        check_msg(
            msg.reply(&ctx.http, "Requires a valid voice channel ID be given")
                .await,
        );

        return Ok(());
    };

    let guild_id = msg.guild_id.unwrap();
    let manager = &ctx.data::<UserData>().songbird;

    // Some events relating to voice receive fire *while joining*.
    // We must make sure that any event handlers are installed before we attempt to join.
    {
        let handler_lock = manager.get_or_insert(guild_id);
        let mut handler = handler_lock.lock().await;

        let evt_receiver = Receiver::new();

        handler.add_global_event(CoreEvent::SpeakingStateUpdate.into(), evt_receiver.clone());
        handler.add_global_event(CoreEvent::RtpPacket.into(), evt_receiver.clone());
        handler.add_global_event(CoreEvent::RtcpPacket.into(), evt_receiver.clone());
        handler.add_global_event(CoreEvent::ClientDisconnect.into(), evt_receiver.clone());
        handler.add_global_event(CoreEvent::VoiceTick.into(), evt_receiver);
    }

    if manager.join(guild_id, connect_to).await.is_ok() {
        check_msg(
            msg.channel_id
                .say(&ctx.http, &format!("Joined {}", connect_to.mention()))
                .await,
        );
    } else {
        // Although we failed to join, we need to clear out existing event handlers on the call.
        _ = manager.remove(guild_id).await;

        check_msg(
            msg.channel_id
                .say(&ctx.http, "Error joining the channel")
                .await,
        );
    }

    Ok(())
}

async fn leave(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();

    let manager = &ctx.data::<UserData>().songbird;
    let has_handler = manager.get(guild_id).is_some();

    if has_handler {
        if let Err(e) = manager.remove(guild_id).await {
            check_msg(
                msg.channel_id
                    .say(&ctx.http, format!("Failed: {:?}", e))
                    .await,
            );
        }

        check_msg(msg.channel_id.say(&ctx.http, "Left voice channel").await);
    } else {
        check_msg(msg.reply(&ctx.http, "Not in a voice channel").await);
    }

    Ok(())
}

async fn ping(ctx: &Context, msg: &Message) -> CommandResult {
    check_msg(msg.channel_id.say(&ctx.http, "Pong!").await);

    Ok(())
}

/// Checks that a message successfully sent; if not, then logs why to stdout.
fn check_msg(result: SerenityResult<Message>) {
    if let Err(why) = result {
        println!("Error sending message: {:?}", why);
    }
}

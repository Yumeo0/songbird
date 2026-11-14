use super::*;
use davey::{CommitWelcome, DaveSession, MediaType, ProposalsOperationType};
use futures::StreamExt;
use openmls::prelude::{tls_codec::Serialize, *};
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;
use serenity_voice_model::payload::{
    ClientsConnect,
    DaveExecuteTransition,
    DaveMlsAnnounceCommitTransition,
    DaveMlsWelcome,
    DavePrepareEpoch,
    DavePrepareTransition,
};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{accept_async, WebSocketStream};

const USER_A: u64 = 1234;
const USER_B: u64 = 5678;
const CHANNEL: u64 = 9012;

// Act as the voice gateway's MLS external sender, using real key packages,
// proposals, commits and welcomes. No Discord connection or accounts are used.
fn joining_sessions() -> (DaveSession, DaveSession, CommitWelcome) {
    let provider = OpenMlsRustCrypto::default();
    let ciphersuite = Ciphersuite::MLS_128_DHKEMP256_AES128GCM_SHA256_P256;
    let signer = SignatureKeyPair::new(ciphersuite.signature_algorithm()).unwrap();
    let external = ExternalSender::new(
        signer.public().into(),
        BasicCredential::new(vec![0, 1, 1, 0]).into(),
    )
    .tls_serialize_detached()
    .unwrap();
    let mut creator = DaveSession::new(NonZeroU16::new(1).unwrap(), USER_A, CHANNEL, None).unwrap();
    let mut joiner = DaveSession::new(NonZeroU16::new(1).unwrap(), USER_B, CHANNEL, None).unwrap();
    creator.set_external_sender(&external).unwrap();
    joiner.set_external_sender(&external).unwrap();
    let key_package =
        KeyPackageIn::tls_deserialize_exact_bytes(&joiner.create_key_package().unwrap())
            .unwrap()
            .validate(provider.crypto(), ProtocolVersion::Mls10)
            .unwrap();
    let proposal = ExternalProposal::new_add::<OpenMlsRustCrypto>(
        key_package,
        GroupId::from_slice(&CHANNEL.to_be_bytes()),
        GroupEpoch::from(0),
        &signer,
        SenderExtensionIndex::new(0),
    )
    .unwrap();
    let proposals = VLBytes::new(proposal.tls_serialize_detached().unwrap())
        .tls_serialize_detached()
        .unwrap();
    let exchange = creator
        .process_proposals(
            ProposalsOperationType::APPEND,
            &proposals,
            Some(&[USER_A, USER_B]),
        )
        .unwrap()
        .unwrap();
    (creator, joiner, exchange)
}

async fn network(
    session: DaveSession,
) -> (
    AuxNetwork,
    Interconnect,
    WebSocketStream<tokio::net::TcpStream>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let (client, server) = tokio::join!(WsStream::connect(url), async {
        accept_async(listener.accept().await.unwrap().0).await
    });
    let info = ConnectionInfo {
        user_id: crate::id::UserId::new(session.user_id()),
        channel_id: crate::id::ChannelId::new(CHANNEL),
        guild_id: crate::id::GuildId::new(1),
        endpoint: "localhost".into(),
        session_id: "offline-test".into(),
        token: String::new(),
    };
    let network = AuxNetwork::new(
        flume::unbounded().1,
        client.unwrap(),
        1,
        1000.0,
        0,
        info,
        Arc::new(RwLock::new(Some(session))),
        Arc::new(AtomicU16::new(1)),
        #[cfg(feature = "receive")]
        Arc::default(),
    );
    let interconnect = Interconnect {
        core: flume::unbounded().0,
        events: flume::unbounded().0,
        mixer: flume::unbounded().0,
    };
    (network, interconnect, server.unwrap())
}

async fn expect_ready(
    peer: &mut WebSocketStream<tokio::net::TcpStream>,
    transition_id: u16,
    version: u16,
) {
    let message = timeout(Duration::from_secs(2), peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event: GatewayEvent = serde_json::from_str(message.to_text().unwrap()).unwrap();
    let GatewayEvent::DaveTransitionReady(ready) = event else {
        panic!("Expected transition ready, received {event:?}");
    };
    assert_eq!(
        ready,
        DaveTransitionReady {
            transition_id,
            protocol_version: version,
        }
    );
}

fn check_bidirectional_audio(a: &AuxNetwork, b: &AuxNetwork) {
    use opus2::{Application, Channels, Encoder};

    let mut encoder = Encoder::new(48000, Channels::Stereo, Application::LowDelay).unwrap();
    let mut opus = vec![0; 4000];
    let len = encoder.encode_float(&[0.25; 1920], &mut opus).unwrap();
    opus.truncate(len);
    for (sender, receiver) in [(a, b), (b, a)] {
        let encrypted = sender
            .dave_session
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .encrypt_opus(&opus)
            .unwrap()
            .into_owned();
        assert_ne!(encrypted, opus);
        let decrypted = receiver
            .dave_session
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .decrypt(sender.info.user_id.get(), MediaType::AUDIO, &encrypted)
            .unwrap();
        assert_eq!(decrypted, opus);
    }
}

#[tokio::test]
async fn dave_transitions_and_reinit_preserve_bidirectional_media() {
    for transition_id in [0, 7] {
        let (creator, joiner, exchange) = joining_sessions();
        let (mut a, interconnect, mut peer_a) = network(creator).await;
        let (mut b, _, mut peer_b) = network(joiner).await;
        for network in [&mut a, &mut b] {
            network
                .process_ws(
                    &interconnect,
                    GatewayEvent::ClientsConnect(ClientsConnect {
                        user_ids: vec![UserId(USER_A), UserId(USER_B)],
                    }),
                )
                .await
                .unwrap();
        }
        a.process_ws(
            &interconnect,
            GatewayEvent::DaveMlsAnnounceCommitTransition(DaveMlsAnnounceCommitTransition {
                transition_id,
                commit_message: exchange.commit,
            }),
        )
        .await
        .unwrap();
        b.process_ws(
            &interconnect,
            GatewayEvent::DaveMlsWelcome(DaveMlsWelcome {
                transition_id,
                welcome: exchange.welcome.unwrap(),
            }),
        )
        .await
        .unwrap();

        if transition_id != 0 {
            // Preparing receive keys must not switch the sender's key yet.
            for network in [&a, &b] {
                assert!(!network
                    .dave_session
                    .read()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .can_encrypt());
            }
            expect_ready(&mut peer_a, transition_id, 1).await;
            expect_ready(&mut peer_b, transition_id, 1).await;
            for network in [&mut a, &mut b] {
                network
                    .process_ws(
                        &interconnect,
                        GatewayEvent::DaveExecuteTransition(DaveExecuteTransition {
                            transition_id,
                        }),
                    )
                    .await
                    .unwrap();
            }
        }
        check_bidirectional_audio(&a, &b);

        for (network, peer) in [(&mut a, &mut peer_a), (&mut b, &mut peer_b)] {
            network
                .process_ws(
                    &interconnect,
                    GatewayEvent::DavePrepareEpoch(DavePrepareEpoch {
                        epoch: 1,
                        protocol_version: 1,
                    }),
                )
                .await
                .unwrap();
            let key_package = timeout(Duration::from_secs(2), peer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(key_package.is_binary());

            // Commits ignored while awaiting proposals must not acknowledge a
            // transition, send an invalid-commit report or reinitialise again.
            network
                .process_ws(
                    &interconnect,
                    GatewayEvent::DaveMlsAnnounceCommitTransition(
                        DaveMlsAnnounceCommitTransition {
                            transition_id: 8,
                            commit_message: Vec::new(),
                        },
                    ),
                )
                .await
                .unwrap();
            assert!(timeout(Duration::from_millis(20), peer.next())
                .await
                .is_err());
        }
        check_bidirectional_audio(&a, &b);

        for (network, peer) in [(&mut a, &mut peer_a), (&mut b, &mut peer_b)] {
            network
                .process_ws(
                    &interconnect,
                    GatewayEvent::DavePrepareTransition(DavePrepareTransition {
                        transition_id: 9,
                        protocol_version: 0,
                    }),
                )
                .await
                .unwrap();
            expect_ready(peer, 9, 0).await;
            assert!(network
                .dave_session
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .can_encrypt());
            network
                .process_ws(
                    &interconnect,
                    GatewayEvent::DaveExecuteTransition(DaveExecuteTransition { transition_id: 9 }),
                )
                .await
                .unwrap();
            assert!(!network
                .dave_session
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .can_encrypt());
            assert_eq!(network.dave_protocol_version.load(Ordering::Relaxed), 0);
        }
    }
}

#[tokio::test]
async fn dave_welcome_rejects_unrecognized_members() {
    let (_, joiner, exchange) = joining_sessions();
    let (mut network, _, _peer) = network(joiner).await;
    assert!(matches!(
        network.dave_process_welcome(&exchange.welcome.unwrap()),
        Some(Err(davey::errors::ProcessWelcomeError::UnrecognizedUser(
            USER_A
        )))
    ));
}

//! Example demonstrating how to store and convert audio streams which you
//! either want to reuse between servers, or to seek/loop on. See `join`, and `ting`.
//!
//! Requires the "cache", "gateway", and "voice" features be enabled in your
//! Cargo.toml, like so:
//!
//! ```toml
//! [dependencies.serenity]
//! git = "https://github.com/serenity-rs/serenity.git"
//! branch = "next"
//! features = ["gateway", "cache", "voice"]
//! ```
use std::sync::{Arc, Weak};

use serenity::all::{FullEvent, Token};
use serenity::{
    async_trait,
    gateway::client::{Client, Context, EventHandler},
    model::channel::Message,
    prelude::{GatewayIntents, Mentionable, Mutex},
    Result as SerenityResult,
};

use songbird::{
    driver::Bitrate,
    input::{
        cached::{Compressed, Memory},
        File,
        Input,
    },
    Call,
    Event,
    EventContext,
    EventHandler as VoiceEventHandler,
    TrackEvent,
};

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
                let (command, _args) = content
                    .split_once(char::is_whitespace)
                    .unwrap_or((content, ""));
                if msg.guild_id.is_none() && command != "ping" {
                    return;
                }
                let result = match command {
                    "deafen" => deafen(ctx, msg).await,
                    "join" => join(ctx, msg).await,
                    "leave" => leave(ctx, msg).await,
                    "mute" => mute(ctx, msg).await,
                    "ting" => ting(ctx, msg).await,
                    "undeafen" => undeafen(ctx, msg).await,
                    "unmute" => unmute(ctx, msg).await,
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

enum CachedSound {
    Compressed(Compressed),
    Uncompressed(Memory),
}

impl From<&CachedSound> for Input {
    fn from(obj: &CachedSound) -> Self {
        use CachedSound::*;
        match obj {
            Compressed(c) => c.new_handle().into(),
            Uncompressed(u) => u.new_handle().into(),
        }
    }
}

struct UserData {
    songbird: Arc<songbird::Songbird>,
    sound_store: dashmap::DashMap<String, CachedSound>,
}

async fn setup_cached_audio() -> dashmap::DashMap<String, CachedSound> {
    // Loading the audio ahead of time.
    let audio_map = dashmap::DashMap::new();

    // Creation of an in-memory source.
    //
    // This is a small sound effect, so storing the whole thing is relatively cheap.
    //
    // `spawn_loader` creates a new thread which works to copy all the audio into memory
    // ahead of time. We do this in both cases to ensure optimal performance for the audio
    // core.
    let ting_src = Memory::new(File::new("../../../resources/ting.wav").into())
        .await
        .expect("These parameters are well-defined.");
    let _ = ting_src.raw.spawn_loader();
    audio_map.insert("ting".into(), CachedSound::Uncompressed(ting_src));

    // Another short sting, to show where each loop occurs.
    let loop_src = Memory::new(File::new("../../../resources/loop.wav").into())
        .await
        .expect("These parameters are well-defined.");
    let _ = loop_src.raw.spawn_loader();
    audio_map.insert("loop".into(), CachedSound::Uncompressed(loop_src));

    // Creation of a compressed source.
    //
    // This is a full song, making this a much less memory-heavy choice.
    //
    // Music by Cloudkicker, used under CC BY-SC-SA 3.0 (https://creativecommons.org/licenses/by-nc-sa/3.0/).
    let song_src = Compressed::new(
        File::new("../../../resources/Cloudkicker - 2011 07.mp3").into(),
        Bitrate::Bits(128_000),
    )
    .await
    .expect("These parameters are well-defined.");
    let _ = song_src.raw.spawn_loader();

    // Compressed sources are internally stored as DCA1 format files.
    // Because `Compressed` implements `std::io::Read`, we can save these
    // to disk and use them again later if we want!
    let mut creator = song_src.new_handle();
    std::thread::spawn(move || {
        let mut out_file = std::fs::File::create("ckick-dca1.dca").unwrap();
        std::io::copy(&mut creator, &mut out_file).expect("Error writing out song!");
    });

    audio_map.insert("song".into(), CachedSound::Compressed(song_src));
    audio_map
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Configure the client with your Discord bot token in the environment.
    let token =
        Token::from_env("DISCORD_TOKEN").expect("Expected a valid token in the environment");

    let intents = GatewayIntents::non_privileged() | GatewayIntents::MESSAGE_CONTENT;

    let manager = songbird::Songbird::serenity();
    let user_data = UserData {
        sound_store: setup_cached_audio().await,
        songbird: Arc::clone(&manager),
    };

    let mut client = Client::builder(token, intents)
        .voice_manager(manager)
        .data(Arc::new(user_data))
        .event_handler(Arc::new(Handler))
        .await
        .expect("Err creating client");

    let _ = client
        .start()
        .await
        .map_err(|why| println!("Client ended: {:?}", why));
}

async fn deafen(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();
    let manager = &ctx.data::<UserData>().songbird;

    let handler_lock = match manager.get(guild_id) {
        Some(handler) => handler,
        None => {
            check_msg(msg.reply(&ctx.http, "Not in a voice channel").await);

            return Ok(());
        },
    };

    let mut handler = handler_lock.lock().await;

    if handler.is_deaf() {
        check_msg(msg.channel_id.say(&ctx.http, "Already deafened").await);
    } else {
        if let Err(e) = handler.deafen(true).await {
            check_msg(
                msg.channel_id
                    .say(&ctx.http, format!("Failed: {:?}", e))
                    .await,
            );
        }

        check_msg(msg.channel_id.say(&ctx.http, "Deafened").await);
    }

    Ok(())
}

async fn join(ctx: &Context, msg: &Message) -> CommandResult {
    let (guild_id, channel_id) = {
        let guild = msg.guild(&ctx.cache).unwrap();
        let channel_id = guild
            .voice_states
            .get(&msg.author.id)
            .and_then(|voice_state| voice_state.channel_id);

        (guild.id, channel_id)
    };

    let connect_to = match channel_id {
        Some(channel) => channel,
        None => {
            check_msg(msg.reply(&ctx.http, "Not in a voice channel").await);

            return Ok(());
        },
    };

    let data = ctx.data::<UserData>();
    if let Ok(handler_lock) = data.songbird.join(guild_id, connect_to).await {
        let call_lock_for_evt = Arc::downgrade(&handler_lock);

        let mut handler = handler_lock.lock().await;
        check_msg(
            msg.channel_id
                .say(&ctx.http, &format!("Joined {}", connect_to.mention()))
                .await,
        );

        let source = data
            .sound_store
            .get("song")
            .map(|s| s.value().into())
            .expect("Handle placed into cache at startup.");

        let song = handler.play_input(source);
        let _ = song.set_volume(1.0);
        let _ = song.enable_loop();

        // Play a guitar chord whenever the main backing track loops.
        let _ = song.add_event(
            Event::Track(TrackEvent::Loop),
            LoopPlaySound {
                call_lock: call_lock_for_evt,
                data,
            },
        );
    } else {
        check_msg(
            msg.channel_id
                .say(&ctx.http, "Error joining the channel")
                .await,
        );
    }

    Ok(())
}

struct LoopPlaySound {
    call_lock: Weak<Mutex<Call>>,
    data: Arc<UserData>,
}

#[async_trait]
impl VoiceEventHandler for LoopPlaySound {
    async fn act(&self, _ctx: &EventContext<'_>) -> Option<Event> {
        if let Some(call_lock) = self.call_lock.upgrade() {
            let src = {
                self.data
                    .sound_store
                    .get("loop")
                    .map(|c| c.value().into())
                    .expect("Handle placed into cache at startup.")
            };

            let mut handler = call_lock.lock().await;
            let sound = handler.play_input(src);
            let _ = sound.set_volume(0.5);
        }

        None
    }
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

async fn mute(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();
    let manager = &ctx.data::<UserData>().songbird;

    let handler_lock = match manager.get(guild_id) {
        Some(handler) => handler,
        None => {
            check_msg(msg.reply(&ctx.http, "Not in a voice channel").await);

            return Ok(());
        },
    };

    let mut handler = handler_lock.lock().await;

    if handler.is_mute() {
        check_msg(msg.channel_id.say(&ctx.http, "Already muted").await);
    } else {
        if let Err(e) = handler.mute(true).await {
            check_msg(
                msg.channel_id
                    .say(&ctx.http, format!("Failed: {:?}", e))
                    .await,
            );
        }

        check_msg(msg.channel_id.say(&ctx.http, "Now muted").await);
    }

    Ok(())
}

async fn ting(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();
    let data = ctx.data::<UserData>();

    if let Some(handler_lock) = data.songbird.get(guild_id) {
        let mut handler = handler_lock.lock().await;

        let source = data
            .sound_store
            .get("ting")
            .map(|c| c.value().into())
            .expect("Handle placed into cache at startup.");

        let _sound = handler.play_input(source);

        check_msg(msg.channel_id.say(&ctx.http, "Ting!").await);
    } else {
        check_msg(
            msg.channel_id
                .say(&ctx.http, "Not in a voice channel to play in")
                .await,
        );
    }

    Ok(())
}

async fn undeafen(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();

    let manager = &ctx.data::<UserData>().songbird;
    if let Some(handler_lock) = manager.get(guild_id) {
        let mut handler = handler_lock.lock().await;

        if let Err(e) = handler.deafen(false).await {
            check_msg(
                msg.channel_id
                    .say(&ctx.http, format!("Failed: {:?}", e))
                    .await,
            );
        }

        check_msg(msg.channel_id.say(&ctx.http, "Undeafened").await);
    } else {
        check_msg(
            msg.channel_id
                .say(&ctx.http, "Not in a voice channel to undeafen in")
                .await,
        );
    }

    Ok(())
}

async fn unmute(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();
    let manager = &ctx.data::<UserData>().songbird;

    if let Some(handler_lock) = manager.get(guild_id) {
        let mut handler = handler_lock.lock().await;

        if let Err(e) = handler.mute(false).await {
            check_msg(
                msg.channel_id
                    .say(&ctx.http, format!("Failed: {:?}", e))
                    .await,
            );
        }

        check_msg(msg.channel_id.say(&ctx.http, "Unmuted").await);
    } else {
        check_msg(
            msg.channel_id
                .say(&ctx.http, "Not in a voice channel to unmute in")
                .await,
        );
    }

    Ok(())
}

/// Checks that a message successfully sent; if not, then logs why to stdout.
fn check_msg(result: SerenityResult<Message>) {
    if let Err(why) = result {
        println!("Error sending message: {:?}", why);
    }
}

use std::time::Duration;

use nt_client::{Client, NTAddr, NewClientOptions};
use tracing::{error, info, level_filters::LevelFilter};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();

    let client = Client::new(NewClientOptions {
        addr: NTAddr::Local,
        // custom WSL ip
        // addr: NTAddr::Custom(Ipv4Addr::new(172, 30, 64, 1)),
        ..Default::default()
    });

    // publishes to `/counter` and increments its value by 1 every second
    let gpub = client.generic_publisher();
    tokio::spawn(async move {
        let mut counter = 0;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            counter += 1;

            info!("updated counter to {counter}");

            // Use a proper integer value instead of trying to use rmpv directly
            if let Err(e) = gpub.set("/counter", counter).await {
                eprintln!("Failed to publish: {e}");
            }

            // existing topic
            if let Err(e) = gpub
                .set("/Shuffleboard/RobotContainer/testBool", counter % 2 == 0)
                .await
            {
                eprintln!("Failed to publish: {e}");
            }
        }
    });

    client.connect().await.map_err(|e| error!("{e}")).unwrap();
}

//! Publisher portion of the `NetworkTables` spec.
//!
//! Publishers are used to set new values for topics that can be seen by subscribers.
//!
//! # Examples
//!
//! ```no_run
//! use std::time::Duration;
//! use nt_client::Client;
//!
//! # tokio_test::block_on(async {
//! let client = Client::new(Default::default());
//!
//! // increments the `/counter` topic every 5 seconds
//! let counter_topic = client.topic("/counter");
//! tokio::spawn(async move {
//!     const INCREMENT_INTERVAL: Duration = Duration::from_secs(5);
//!
//!     let mut publisher = counter_topic.publish::<u32>(Default::default()).await.unwrap();
//!     let mut interval = tokio::time::interval(INCREMENT_INTERVAL);
//!     let mut counter = 0;
//!
//!     loop {
//!         interval.tick().await;
//!
//!         publisher.set(counter).await.expect("connection is still alive");
//!         counter += 1;
//!     }
//! });
//!
//! client.connect().await.unwrap();
//! # });

use std::{collections::HashMap, fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};

use tokio::sync::{broadcast, RwLock};
use tracing::debug;

use crate::{
    data::{
        r#type::{DataType, NetworkTableData},
        Announce, BinaryData, ClientboundData, ClientboundTextData, Properties, PropertiesData,
        Publish, ServerboundMessage, ServerboundTextData, SetProperties, Unpublish,
    },
    error::ConnectionClosedError,
    recv_until, Client, NTClientReceiver, NTServerSender, NetworkTablesTime,
};

/// A `NetworkTables` publisher that publishes values to a [`Topic`].
///
/// This will automatically get unpublished whenever this goes out of scope.
///
/// [`Topic`]: crate::topic::Topic
pub struct Publisher<T: NetworkTableData> {
    _phantom: PhantomData<T>,
    topic: String,
    id: i32,
    time: Arc<RwLock<NetworkTablesTime>>,
    ws_sender: NTServerSender,
    ws_recv: NTClientReceiver,
}

impl<T: NetworkTableData> Debug for Publisher<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("id", &self.id)
            .field("type", &T::data_type())
            .finish()
    }
}

impl<T: NetworkTableData> PartialEq for Publisher<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T: NetworkTableData> Eq for Publisher<T> {}

impl<T: NetworkTableData> Publisher<T> {
    pub(super) async fn new(
        name: String,
        properties: Properties,
        time: Arc<RwLock<NetworkTablesTime>>,
        ws_sender: NTServerSender,
        mut ws_recv: NTClientReceiver,
    ) -> Result<Self, NewPublisherError> {
        let id = rand::random();
        let pub_message = ServerboundTextData::Publish(Publish {
            name,
            pubuid: id,
            r#type: T::data_type(),
            properties,
        });
        ws_sender
            .send(ServerboundMessage::Text(pub_message).into())
            .map_err(|_| broadcast::error::RecvError::Closed)?;

        let (name, r#type, id) = {
            recv_until(&mut ws_recv, |data| {
                if let ClientboundData::Text(ClientboundTextData::Announce(Announce {
                    ref name,
                    ref r#type,
                    pubuid: Some(pubuid),
                    ..
                })) = *data
                {
                    // TODO: cached property

                    Some((name.clone(), r#type.clone(), pubuid))
                } else {
                    None
                }
            })
            .await
        }?;
        if T::data_type() != r#type {
            return Err(NewPublisherError::MismatchedType {
                server: r#type,
                client: T::data_type(),
            });
        };

        debug!("[pub {id}] publishing to topic `{name}`");
        Ok(Self {
            _phantom: PhantomData,
            topic: name,
            id,
            time,
            ws_sender,
            ws_recv,
        })
    }

    /// Publish a new value to the [`Topic`].
    ///
    /// [`Topic`]: crate::topic::Topic
    pub async fn set(&self, value: T) -> Result<(), ConnectionClosedError> {
        let time = self.time.read().await;
        self.set_time(value, time.server_time()).await
    }

    /// Publishes a default value to the [`Topic`].
    ///
    /// This default value will only be seen by other clients and the server if no other value has
    /// been published to the [`Topic`] yet.
    ///
    /// [`Topic`]: crate::topic::Topic
    pub async fn set_default(&self, value: T) -> Result<(), ConnectionClosedError> {
        self.set_time(value, Duration::ZERO).await
    }

    /// Updates the properties of the topic being subscribed to, returning a `future` that
    /// completes when the server acknowledges the update.
    ///
    /// A [`SetPropsBuilder`] should be used for easy creation of updated properties.
    ///
    /// # Errors
    /// Returns an error if messages could not be received from the `NetworkTables` server.
    ///
    /// # Examples
    /// ```no_run
    /// use std::time::Duration;
    /// use nt_client::{publish::UpdateProps, Client};
    ///
    /// # tokio_test::block_on(async {
    /// let client = Client::new(Default::default());
    ///
    /// let topic = client.topic("mytopic");
    /// tokio::spawn(async move {
    ///     let mut sub = topic.publish::<String>(Default::default()).await.unwrap();
    ///
    ///     // update properties after 5 seconds
    ///     tokio::time::sleep(Duration::from_secs(5)).await;
    ///
    ///     // Props:
    ///     // - set `retained` to true
    ///     // - delete `arbitrary property`
    ///     // everything else stays unchanged
    ///     let props = UpdateProps::new()
    ///         .set_retained(true)
    ///         .delete("arbitrary property".to_owned());
    ///
    ///     sub.update_props(props).await.unwrap();
    /// });
    ///
    /// client.connect().await.unwrap();
    /// # })
    /// ```
    // TODO: probably replace with custom error
    pub async fn update_props(
        &mut self,
        new_props: UpdateProps,
    ) -> Result<(), broadcast::error::RecvError> {
        self.ws_sender
            .send(
                ServerboundMessage::Text(ServerboundTextData::SetProperties(SetProperties {
                    name: self.topic.clone(),
                    update: new_props.into(),
                }))
                .into(),
            )
            .map_err(|_| broadcast::error::RecvError::Closed)?;

        recv_until(&mut self.ws_recv, |data| {
            if let ClientboundData::Text(ClientboundTextData::Properties(PropertiesData {
                ref name,
                ..
            })) = *data
            {
                if name != &self.topic {
                    return None;
                };

                Some(())
            } else {
                None
            }
        })
        .await?;

        Ok(())
    }

    async fn set_time(&self, data: T, timestamp: Duration) -> Result<(), ConnectionClosedError> {
        let data_value = data.clone().into_value();
        let binary = BinaryData::new(self.id, timestamp, data);
        self.ws_sender
            .send(ServerboundMessage::Binary(binary).into())
            .map_err(|_| ConnectionClosedError)?;
        debug!(
            "[pub {}] set to {data_value} at time {timestamp:?}",
            self.id
        );
        Ok(())
    }
}

impl<T: NetworkTableData> Drop for Publisher<T> {
    fn drop(&mut self) {
        let data = ServerboundTextData::Unpublish(Unpublish { pubuid: self.id });
        // if the receiver is dropped, the ws connection is closed
        let _ = self.ws_sender.send(ServerboundMessage::Text(data).into());
        debug!("[pub {}] unpublished", self.id);
    }
}

/// Errors that can occur when creating a new [`Publisher`].
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum NewPublisherError {
    /// An error occurred when receiving data from the connection.
    #[error(transparent)]
    // TODO: probably replace with custom error
    Recv(#[from] broadcast::error::RecvError),
    /// The server and client have mismatched data types.
    ///
    /// This can occur if, for example, the client is publishing [`String`]s to a topic that the
    /// server has a different data type for, like an [`i32`].
    #[error("mismatched data types! server has {server:?}, but tried to use {client:?} instead")]
    MismatchedType {
        /// The server's data type.
        server: DataType,
        /// The client's data type.
        client: DataType,
    },
}

macro_rules! builder {
    ($lit: literal : [
        $( #[ $gc_m: meta ] )* fn $get_ckd: ident,
        $( #[ $g_m: meta ] )* fn $get: ident,
        $( #[ $u_m: meta ] )* fn $update: ident,
        $( #[ $s_m: meta ] )* fn $set: ident,
        $( #[ $d_m: meta ] )* fn $delete: ident,
        $( #[ $k_m: meta ] )* fn $keep: ident,
    ] : $ty: ty where
        $as_pat: pat => $as_value: expr,
        $from_pat: pat => $from_value: expr) => {
        $( #[ $gc_m ] )*
        pub fn $get_ckd(&self) -> Option<PropUpdate<&$ty>> {
            match self.get($lit) {
                PropUpdate::Set($from_pat) => Some(PropUpdate::Set($from_value)),
                PropUpdate::Set(_) => None,
                PropUpdate::Delete => Some(PropUpdate::Delete),
                PropUpdate::Keep => Some(PropUpdate::Keep),
            }
        }

        $( #[ $g_m ] )*
        pub fn $get(&self) -> PropUpdate<&$ty> {
            match self.$get_ckd() {
                Some(value) => value,
                None => panic!("invalid `{}` value", $lit)
            }
        }

        $( #[ $u_m ] )*
        pub fn $update(self, value: PropUpdate<$ty>) -> Self {
            let value = match value {
                PropUpdate::Set($as_pat) => PropUpdate::Set($as_value),
                PropUpdate::Delete => PropUpdate::Delete,
                PropUpdate::Keep => PropUpdate::Keep,
            };
            self.update($lit.to_owned(), value)
        }
        $( #[ $s_m ] )*
        pub fn $set(self, value: $ty) -> Self {
            let $as_pat = value;
            let value = $as_value;
            self.set($lit.to_owned(), value)
        }
        $( #[ $d_m ] )*
        pub fn $delete(self) -> Self {
            self.delete($lit.to_owned())
        }
        $( #[ $k_m ] )*
        pub fn $keep(self) -> Self {
            self.keep($lit.to_owned())
        }
    };
}

/// Properties to update on a topic.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct UpdateProps {
    inner: HashMap<String, Option<serde_json::Value>>,
}

impl UpdateProps {
    /// Creates a new property update. All properties are `kept` by default.
    ///
    /// This is identical to calling [`Default::default`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates a property update updating certain server-recognized properties.
    ///
    /// This method differs from [`with_props_unchange`][`Self::with_props_unchange`] because
    /// unlike that method, this method deletes the property if it's [`None`].
    ///
    /// With the `extra` field, if the key is not present in the map, it does not get updated. If
    /// the key is present, it gets set to the value.
    ///
    /// Behavior with the `extra` field is identical in both methods.
    ///
    /// # Examples
    /// ```
    /// use nt_client::{data::Properties, publish::UpdateProps};
    ///
    /// // properties are:
    /// // - persistent: `true`
    /// // - cached: `true`
    /// // - retained: unset (defaults to `false`)
    /// let properties = Properties { persistent: Some(true), cached: Some(true), ..Default::default() };
    ///
    /// // update properties is:
    /// // - set `persistent` to `true`
    /// // - set `cached` to `true`
    /// // - delete `retained`
    /// // everything else stays unchanged
    /// let builder = UpdateProps::with_props_delete(properties);
    /// ```
    pub fn with_props_delete(
        Properties {
            persistent,
            retained,
            cached,
            extra,
        }: Properties,
    ) -> Self {
        let mut update = Self::new()
            .update_persistent(PropUpdate::from_option_delete(persistent))
            .update_retained(PropUpdate::from_option_delete(retained))
            .update_cached(PropUpdate::from_option_delete(cached));

        for (key, value) in extra {
            update = update.set(key, value);
        }

        update
    }

    /// Creates a new property update updating certain server-recognized properties.
    ///
    /// This method differs from [`with_props_delete`][`Self::with_props_delete`] because
    /// unlike that method, this method does not delete the property if it's [`None`]. Rather, it
    /// keeps it unchanged.
    ///
    /// With the `extra` field, if the key is not present in the map, it does not get updated. If
    /// the key is present, it gets set to the value.
    ///
    /// Behavior with the `extra` field is identical in both methods.
    ///
    /// # Examples
    /// ```
    /// use nt_client::{data::Properties, publish::UpdateProps};
    ///
    /// // properties are:
    /// // - persistent: `true`
    /// // - cached: `true`
    /// // - retained: unset (defaults to `false`)
    /// let properties = Properties { persistent: Some(true), cached: Some(true), ..Default::default() };
    ///
    /// // update properties is:
    /// // - set `persistent` to `true`
    /// // - set `cached` to `true`
    /// // everything else stays unchanged
    /// let builder = UpdateProps::with_props_keep(properties);
    /// ```
    pub fn with_props_keep(
        Properties {
            persistent,
            retained,
            cached,
            extra,
        }: Properties,
    ) -> Self {
        let mut update = Self::new()
            .update_persistent(PropUpdate::from_option_keep(persistent))
            .update_retained(PropUpdate::from_option_keep(retained))
            .update_cached(PropUpdate::from_option_keep(cached));

        for (key, value) in extra {
            update = update.set(key, value);
        }

        update
    }

    /// Gets how a property will update.
    pub fn get(&self, key: &str) -> PropUpdate<&serde_json::Value> {
        match self.inner.get(key) {
            Some(Some(value)) => PropUpdate::Set(value),
            Some(None) => PropUpdate::Delete,
            None => PropUpdate::Keep,
        }
    }

    /// Updates a property.
    ///
    /// See also:
    /// - [`set`][`Self::set`]
    /// - [`delete`][`Self::delete`]
    /// - [`keep`][`Self::keep`]
    pub fn update(mut self, key: String, update: PropUpdate<serde_json::Value>) -> Self {
        match update {
            PropUpdate::Set(value) => self.inner.insert(key, Some(value)),
            PropUpdate::Delete => self.inner.insert(key, None),
            PropUpdate::Keep => self.inner.remove(&key),
        };
        self
    }

    /// Sets a new value to the property.
    ///
    /// This is the same as calling `update(key, PropUpdate::Set(value))`.
    pub fn set(self, key: String, value: serde_json::Value) -> Self {
        self.update(key, PropUpdate::Set(value))
    }

    /// Deletes a property on the server.
    ///
    /// This is the same as calling `update(key, PropUpdate::Delete)`.
    pub fn delete(self, key: String) -> Self {
        self.update(key, PropUpdate::Delete)
    }

    /// Keeps a property on the server, leaving its current value unchanged.
    ///
    /// By default, all properties are `kept`.
    ///
    /// This is the same as calling `update(key, PropUpdate::Keep)`.
    pub fn keep(self, key: String) -> Self {
        self.update(key, PropUpdate::Keep)
    }

    builder!("persistent": [
        /// Gets the server-recognized `persistent` property, checking whether it is a [`bool`] or
        /// not.
        ///
        /// A return value of [`None`] indicates that the `persistent` property is not a [`bool`].
        ///
        /// See the [`get`][`Self::get`] documentation for more info.
        ///
        /// This is the checked version of [`persistent`][`Self::persistent`].
        fn persistent_checked,
        /// Gets the server-recognized `persistent` property.
        ///
        /// See the [`get`][`Self::get`] documentation for more info.
        ///
        /// # Panics
        ///
        /// Panics if the `persistent` property is not set to a boolean.
        fn persistent,
        /// Updates the server-recognized `persistent` property.
        ///
        /// See the [`update`][`Self::update`] documentation for more info.
        fn update_persistent,
        /// Sets the server-recognized `persistent` property.
        ///
        /// See the [`set`][`Self::set`] documentation for more info.
        fn set_persistent,
        /// Deletes the server-recognized `persistent` property.
        ///
        /// See the [`delete`][`Self::delete`] documentation for more info.
        fn delete_persistent,
        /// Keeps the server-recognized `persistent` property.
        ///
        /// See the [`keep`][`Self::keep`] documentation for more info.
        fn keep_persistent,
    ]: bool where
        bool => serde_json::Value::Bool(bool),
        serde_json::Value::Bool(value) => value);

    builder!("retained": [
        /// Gets the server-recognized `retained` property, checking whether it is a [`bool`] or
        /// not.
        ///
        /// A return value of [`None`] indicates that the `retained` property is not a [`bool`].
        ///
        /// See the [`get`][`Self::get`] documentation for more info.
        ///
        /// This is the checked version of [`retained`][`Self::retained`].
        fn retained_checked,
        /// Gets the server-recognized `retained` property.
        ///
        /// See the [`get`][`Self::get`] documentation for more info.
        ///
        /// # Panics
        ///
        /// Panics if the `retained` property is not set to a boolean.
        fn retained,
        /// Updates the server-recognized `retained` property.
        ///
        /// See the [`update`][`Self::update`] documentation for more info.
        fn update_retained,
        /// Sets the server-recognized `retained` property.
        ///
        /// See the [`set`][`Self::set`] documentation for more info.
        fn set_retained,
        /// Deletes the server-recognized `retained` property.
        ///
        /// See the [`delete`][`Self::delete`] documentation for more info.
        fn delete_retained,
        /// Keeps the server-recognized `retained` property.
        ///
        /// See the [`keep`][`Self::keep`] documentation for more info.
        fn keep_retained,
    ]: bool where
        bool => serde_json::Value::Bool(bool),
        serde_json::Value::Bool(value) => value);

    builder!("cached": [
        /// Gets the server-recognized `cached` property, checking whether it is a [`bool`] or
        /// not.
        ///
        /// A return value of [`None`] indicates that the `cached` property is not a [`bool`].
        ///
        /// See the [`get`][`Self::get`] documentation for more info.
        ///
        /// This is the checked version of [`cached`][`Self::cached`].
        fn cached_checked,
        /// Gets the server-recognized `cached` property.
        ///
        /// See the [`get`][`Self::get`] documentation for more info.
        ///
        /// # Panics
        ///
        /// Panics if the `cached` property is not set to a boolean.
        fn cached,
        /// Updates the server-recognized `cached` property.
        ///
        /// See the [`update`][`Self::update`] documentation for more info.
        fn update_cached,
        /// Sets the server-recognized `cached` property.
        ///
        /// See the [`set`][`Self::set`] documentation for more info.
        fn set_cached,
        /// Deletes the server-recognized `cached` property.
        ///
        /// See the [`delete`][`Self::delete`] documentation for more info.
        fn delete_cached,
        /// Keeps the server-recognized `cached` property.
        ///
        /// See the [`keep`][`Self::keep`] documentation for more info.
        fn keep_cached,
    ]: bool where
        bool => serde_json::Value::Bool(bool),
        serde_json::Value::Bool(value) => value);
}

impl From<HashMap<String, Option<serde_json::Value>>> for UpdateProps {
    fn from(value: HashMap<String, Option<serde_json::Value>>) -> Self {
        Self { inner: value }
    }
}

impl From<UpdateProps> for HashMap<String, Option<serde_json::Value>> {
    fn from(value: UpdateProps) -> Self {
        value.inner
    }
}

/// An update to a property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropUpdate<T> {
    /// Sets a new value to the property.
    Set(T),
    /// Deletes the property.
    Delete,
    /// Keeps the property, leaving its value unchanged.
    Keep,
}

impl<T> PropUpdate<T> {
    /// Converts an `Option<T>` to a `PropUpdate<T>`, deleting the property if it is [`None`].
    pub fn from_option_delete(option: Option<T>) -> Self {
        match option {
            Some(t) => Self::Set(t),
            None => Self::Delete,
        }
    }

    /// Converts an `Option<T>` to a `PropUpdate<T>`, keeping the property if it is [`None`].
    pub fn from_option_keep(option: Option<T>) -> Self {
        match option {
            Some(t) => Self::Set(t),
            None => Self::Keep,
        }
    }
}

/// A generic `NetworkTables` publisher that can publish values of any type to any topic.
///
/// Unlike the typed `Publisher<T>`, this publisher can send different data types to different topics
/// without being constrained to a single type.
pub struct GenericPublisher {
    time: Arc<RwLock<NetworkTablesTime>>,
    ws_sender: NTServerSender,
}

impl Debug for GenericPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericPublisher").finish()
    }
}

impl GenericPublisher {
    /// Creates a new `GenericPublisher`
    pub(crate) fn new(time: Arc<RwLock<NetworkTablesTime>>, ws_sender: NTServerSender) -> Self {
        Self { time, ws_sender }
    }

    /// Publishes a value of any type to any topic.
    ///
    /// This method allows publishing to any topic with any data type that implements
    /// `NetworkTableData`, without being constrained to a specific type.
    pub async fn set<K: AsRef<str>, V: NetworkTableData>(
        &self,
        topic_key: K,
        value: V,
    ) -> Result<(), ConnectionClosedError> {
        let topic_str = topic_key.as_ref().to_string();
        debug!(
            "GenericPublisher: publishing to topic `{}` with type {:?}",
            topic_str,
            V::data_type()
        );

        // Create a random ID for this publication
        let temp_id = rand::random::<i32>();

        // Publish message for the temporary topic
        let pub_message = ServerboundTextData::Publish(Publish {
            name: topic_str.clone(),
            pubuid: temp_id,
            r#type: V::data_type(), // Use the correct data type for this value
            properties: Properties::default(),
        });

        self.ws_sender
            .send(ServerboundMessage::Text(pub_message).into())
            .map_err(|_| ConnectionClosedError)?;

        // Get server time
        let time = self.time.read().await;
        let timestamp = time.server_time();

        // Create and send the value - we need to convert the value properly
        let binary = BinaryData::new(temp_id, timestamp, value);
        self.ws_sender
            .send(ServerboundMessage::Binary(binary).into())
            .map_err(|_| ConnectionClosedError)?;

        debug!(
            "GenericPublisher: successfully published to `{}`",
            topic_str
        );
        Ok(())
    }
}

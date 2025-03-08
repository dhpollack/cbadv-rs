use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::{SplitSink, Stream};
use futures::task::{noop_waker_ref, Context, Poll};
use futures::StreamExt;
use serde::de;
use tokio::net::TcpStream;
use tokio::sync::MutexGuard;
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::errors::CbError;
use crate::models::adv::websocket::{
    Endpoint, EndpointStream, EndpointType, WebSocketEndpoints, WebSocketSubscriptions,
};
use crate::token_bucket::TokenBucket;
use crate::types::CbResult;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[async_trait]
pub trait WebSocketClientTrait<M: de::DeserializeOwned + Send, C: de::DeserializeOwned + Send>:
    Sync + Send
{
    fn public_enabled(&self) -> bool;
    fn auth_enabled(&self) -> bool;
    fn uris(&self) -> HashMap<EndpointType, String>;
    fn max_retries(&self) -> u32;
    async fn public_tx_lock(&self) -> MutexGuard<Option<SplitSink<Socket, WsMessage>>>;
    async fn auth_tx_lock(&self) -> MutexGuard<Option<SplitSink<Socket, WsMessage>>>;
    #[allow(private_interfaces)]
    async fn subscriptions_lock(&self) -> MutexGuard<WebSocketSubscriptions>;
    #[allow(private_interfaces)]
    async fn public_bucket_lock(&self) -> MutexGuard<TokenBucket>;
    #[allow(private_interfaces)]
    async fn auth_bucket_lock(&self) -> MutexGuard<TokenBucket>;

    /// Connects to the endpoints specified in the builder. This is required before subscribing to any channels.
    ///
    /// # Errors
    ///
    /// Returns a `CbError` if the WebSocket connection fails.
    async fn connect(&self) -> CbResult<WebSocketEndpoints> {
        let mut endpoints = WebSocketEndpoints::default();

        if self.public_enabled() {
            let endpoint = self.connect_endpoint(&EndpointType::Public).await?;
            endpoints.add(EndpointType::Public, endpoint);
        }

        if self.auth_enabled() {
            let endpoint = self.connect_endpoint(&EndpointType::User).await?;
            endpoints.add(EndpointType::User, endpoint);
        }

        Ok(endpoints)
    }

    /// Connects to the WebSocket endpoint.
    async fn connect_endpoint(&self, endpoint_type: &EndpointType) -> CbResult<Endpoint> {
        match endpoint_type {
            EndpointType::Public => {
                let (public_socket, _) = connect_async(
                    self.uris()
                        .get(endpoint_type)
                        .expect("endpoint type uri not found"),
                )
                .await
                .map_err(|why| {
                    CbError::BadConnection(format!(
                        "Unable to establish public WebSocket connection: {why}",
                    ))
                })?;
                let (public_sink, stream) = public_socket.split();
                {
                    let mut tx = self.public_tx_lock().await;
                    *tx = Some(public_sink);
                }
                Ok(Endpoint::Public((EndpointType::Public, stream)))
            }
            EndpointType::User => {
                let (secure_socket, _) = connect_async(
                    self.uris()
                        .get(endpoint_type)
                        .expect("endpoint type uri not found"),
                )
                .await
                .map_err(|why| {
                    CbError::BadConnection(format!(
                        "Unable to establish secure user WebSocket connection: {why}",
                    ))
                })?;
                let (secure_sink, stream) = secure_socket.split();
                {
                    let mut tx = self.auth_tx_lock().await;
                    *tx = Some(secure_sink);
                }
                Ok(Endpoint::User((EndpointType::User, stream)))
            }
        }
    }
    /// Reconnects to a specific endpoint. Returns the reader of the endpoint.
    ///
    /// # Errors
    ///
    /// Returns a `CbError` if the WebSocket connection fails.
    async fn handle_reconnect(&mut self, endpoint_type: &EndpointType) -> CbResult<Endpoint>;
    /// Waits for a reconnection to occur. This is used when a WebSocket connection is lost.
    ///
    /// # Errors
    ///
    /// Returns a `CbError` if the WebSocket connection fails or auto-reconnect is disabled.
    async fn wait_on_reconnect(&mut self, endpoint_type: &EndpointType) -> CbResult<Endpoint> {
        let max_retries = self.max_retries();
        if max_retries == 0 {
            return Err(CbError::BadConnection(
                "Auto-reconnect is disabled. Exiting...".to_string(),
            ));
        }

        let mut retries = 0;
        let mut retry_delay = 2;

        // Rety until max retries hit.
        while retries < max_retries {
            match self.handle_reconnect(endpoint_type).await {
                Ok(endpoint) => return Ok(endpoint),
                Err(why) => {
                    eprintln!(
                        "Failed to reconnect WebSocket: {why}. Retrying in {retry_delay} seconds..."
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay)).await;
                    retries += 1;
                    retry_delay = (retry_delay * 2).min(60);
                }
            }
        }

        Err(CbError::BadConnection(format!(
            "Failed to reconnect WebSocket after {retries} attempts."
        )))
    }
    /// Reconnects to the WebSocket endpoint. Returns a new `EndpointStream`.
    /// This is used when the WebSocket connection is lost.
    ///
    /// # Arguments
    ///
    /// * `stream` - The current `EndpointStream` that was being listened to.
    ///
    /// # Errors
    ///
    /// Returns a `CbError` if the WebSocket connection fails.
    async fn reconnect<E>(&mut self, stream: E) -> CbResult<WebSocketEndpoints>
    where
        E: Into<EndpointStream> + Send,
    {
        let mut new_endpoints = WebSocketEndpoints::default();

        match stream.into() {
            EndpointStream::Single(route, _) => {
                // Reconnect and return a new Single EndpointStream.
                match self.wait_on_reconnect(&route).await {
                    Ok(endpoint) => {
                        new_endpoints.add(route, endpoint);
                        Ok(new_endpoints)
                    }
                    Err(why) => Err(why),
                }
            }
            EndpointStream::Multiple(_) => {
                // Obtain all the endpoints that need to be reconnected.
                let keys = {
                    let subs = self.subscriptions_lock().await;
                    subs.get_keys()
                };

                // Iterate over each endpoint and attempt to reconnect.
                for endpoint_type in keys {
                    match self.wait_on_reconnect(&endpoint_type).await {
                        Ok(new_endpoint) => {
                            new_endpoints.add(endpoint_type.clone(), new_endpoint);
                        }
                        Err(why) => {
                            return Err(why);
                        }
                    }
                }

                if new_endpoints.is_empty() {
                    return Err(CbError::BadConnection(
                        "Failed to reconnect to any endpoints.".to_string(),
                    ));
                }

                Ok(new_endpoints)
            }
        }
    }
    /// Listens to WebSocket readers, supporting both single and multiple endpoints.
    ///
    /// # Arguments
    ///
    /// * `endpoints` - A single `Endpoint` or multiple `WebSocketEndpoints`.
    /// * `callback` - The asynchronous closure to invoke on each message.
    async fn listen<E, F, Fut>(&mut self, endpoints: E, mut callback: F)
    where
        E: Into<EndpointStream> + Send,
        F: FnMut(CbResult<M>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let mut stream = endpoints.into();

        loop {
            while let Some(message) = stream.next().await {
                if let Some(result) = Self::process_message(message) {
                    if let Err(CbError::BadConnection(_)) = &result {
                        // Handle reconnection logic.
                        match self.reconnect(stream).await {
                            Ok(new_stream) => {
                                stream = new_stream.into();
                                break; // Exit inner loop to reconnect.
                            }
                            Err(why) => {
                                eprintln!("Failed to reconnect: {why}");
                                return; // Exit function if reconnection fails
                            }
                        }
                    }

                    // Invoke the asynchronous closure with the result.
                    callback(result).await;
                }
            }
        }
    }
    /// Fetches messages from the WebSocket stream with a limit on the number of messages to fetch.
    ///
    /// NOTE: Adequate pauses / sleeps between calls should be added to prevent busy-looping.
    ///
    /// # Arguments
    ///
    /// * `stream` - The WebSocket stream to get messages from.
    /// * `limit` - The maximum number of messages to fetch. Use `usize::MAX` to fetch all messages.
    /// * `action` - The action to take on each message.
    ///
    /// # Errors
    ///
    /// Returns a `String` if the user returns an error within the action.
    fn fetch_sync<F>(
        &self,
        stream: &mut EndpointStream,
        limit: usize,
        mut action: F,
    ) -> Result<(), String>
    where
        F: FnMut(CbResult<M>) -> Result<(), String>,
    {
        let mut count = 0;

        while count <= limit || limit == usize::MAX {
            // Use poll_next to check for available messages without waiting.
            match Pin::new(&mut *stream).poll_next(&mut Context::from_waker(noop_waker_ref())) {
                Poll::Ready(Some(message)) => {
                    // Process and add the message to the result vector if valid.
                    if let Some(result) = Self::process_message(message) {
                        action(result)?;
                    }

                    count += 1;
                }
                Poll::Ready(None) | Poll::Pending => {
                    // No more messages available or stream is pending; exit the loop.
                    break;
                }
            }
        }

        Ok(())
    }

    /// Waits for a token to be consumable for the correct bucket.
    async fn wait_on_bucket(&mut self, endpoint: &EndpointType) {
        match endpoint {
            EndpointType::Public => {
                let mut locked_bucket = self.public_bucket_lock().await;
                locked_bucket.wait_on().await;
            }
            EndpointType::User => {
                let mut locked_bucket = self.auth_bucket_lock().await;
                locked_bucket.wait_on().await;
            }
        }
    }
    /// Processes the WebSocket message and returns a `M` if successful.
    ///
    /// # Arguments
    ///
    /// * `message` - The WebSocket message to process.
    fn process_message(message: Result<WsMessage, WsError>) -> Option<CbResult<M>> {
        match message {
            Ok(msg) => match msg {
                WsMessage::Text(data) => {
                    let result = serde_json::from_str::<M>(&data).map_err(|why| {
                        CbError::BadParse(format!("Unable to parse message: {data}. Error: {why}"))
                    });
                    Some(result)
                }
                WsMessage::Ping(_)
                | WsMessage::Pong(_)
                | WsMessage::Binary(_)
                | WsMessage::Frame(_) => None, // Ignored.
                WsMessage::Close(frame) => {
                    eprintln!("WebSocket closed: {frame:?}");
                    Some(Err(CbError::BadConnection("WebSocket closed".to_string())))
                }
            },
            Err(why) => Some(Err(CbError::BadConnection(format!(
                "WebSocket error: {why}"
            )))),
        }
    }
    async fn update(
        &mut self,
        channel: &C,
        product_ids: &[String],
        action: &str,
        endpoint: &EndpointType,
    ) -> CbResult<()>;
    async fn subscribe(&mut self, channel: &C, product_ids: &[String]) -> CbResult<()>;
    async fn unsubscribe(&mut self, channel: &C, product_ids: &[String]) -> CbResult<()>;
}

pub trait WebSocketClientAsyncTrait<M: de::DeserializeOwned + Send, C: de::DeserializeOwned + Send>:
    WebSocketClientTrait<M, C>
{
    /// Asynchronously fetches messages from the WebSocket stream with a limit on the number of messages to fetch.
    ///
    /// NOTE: Adequate pauses / sleeps between calls should be added to prevent busy-looping.
    ///
    /// # Arguments
    ///
    /// * `stream` - The WebSocket stream to get messages from.
    /// * `limit` - The maximum number of messages to fetch. Use `usize::MAX` to fetch all messages.
    /// * `action` - The action to take on each message.
    ///
    /// # Errors
    ///
    /// Returns a `String` if the user returns an error within the action.
    #[allow(async_fn_in_trait)]
    async fn fetch_async<F, Fut>(
        &self,
        stream: &mut EndpointStream,
        limit: usize,
        mut action: F,
    ) -> Result<(), String>
    where
        F: FnMut(CbResult<M>) -> Fut + Send,
        Fut: Future<Output = Result<(), String>> + Send,
    {
        let mut count = 0;

        while count <= limit || limit == usize::MAX {
            // Use poll_next to check for available messages without waiting.
            match Pin::new(&mut *stream).poll_next(&mut Context::from_waker(noop_waker_ref())) {
                Poll::Ready(Some(message)) => {
                    // Process and add the message to the result vector if valid.
                    if let Some(result) = Self::process_message(message) {
                        action(result).await?;
                    }

                    count += 1;
                }
                Poll::Ready(None) | Poll::Pending => {
                    // No more messages available or stream is pending; exit the loop.
                    break;
                }
            }
        }
        Ok(())
    }
}

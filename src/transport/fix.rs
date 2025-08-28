// src/fix_handler.rs - FIX Protocol Handler for Trading OMS
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    select,
    sync::{broadcast, mpsc, RwLock},
    time::{interval, sleep, timeout, Instant},
};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

// FIX protocol constants
const FIX_VERSION: &str = "FIX.4.4";
const SOH: char = '\x01'; // Start of Header
const MSG_TYPE_HEARTBEAT: &str = "0";
const MSG_TYPE_LOGON: &str = "A";
const MSG_TYPE_LOGOUT: &str = "5";
const MSG_TYPE_NEW_ORDER_SINGLE: &str = "D";
const MSG_TYPE_EXECUTION_REPORT: &str = "8";
const MSG_TYPE_ORDER_CANCEL_REQUEST: &str = "F";

// FIX message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixMessage {
    pub msg_type: String,
    pub order_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub price: Option<f64>,
    pub order_type: String,
}

#[derive(Debug, Clone)]
pub struct ParsedFixMessage {
    pub fields: HashMap<u16, String>,
    pub msg_type: String,
    pub seq_num: u64,
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub sending_time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct FixSession {
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub seq_num_out: Arc<AtomicU64>,
    pub seq_num_in: Arc<AtomicU64>,
    pub is_logged_on: Arc<RwLock<bool>>,
    pub last_heartbeat: Arc<RwLock<Instant>>,
    pub heartbeat_interval: Duration,
}

pub struct FixHandler {
    pub session: FixSession,
    pub message_tx: mpsc::UnboundedSender<FixMessage>,
    pub message_rx: Arc<RwLock<Option<mpsc::UnboundedReceiver<FixMessage>>>>,
    pub inbound_tx: broadcast::Sender<ParsedFixMessage>,
    pub outbound_tx: mpsc::UnboundedSender<String>,
    pub listener_port: u16,
}

impl FixHandler {
    #[instrument]
    pub async fn new(
        sender_comp_id: String,
        target_comp_id: String,
        port: u16,
    ) -> Result<Self> {
        info!("Creating FIX handler: {} -> {}", sender_comp_id, target_comp_id);
        
        let session = FixSession {
            sender_comp_id,
            target_comp_id,
            seq_num_out: Arc::new(AtomicU64::new(1)),
            seq_num_in: Arc::new(AtomicU64::new(1)),
            is_logged_on: Arc::new(RwLock::new(false)),
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            heartbeat_interval: Duration::from_secs(30),
        };
        
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        let (inbound_tx, _) = broadcast::channel(1000);
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        
        let handler = Self {
            session,
            message_tx,
            message_rx: Arc::new(RwLock::new(Some(message_rx))),
            inbound_tx,
            outbound_tx,
            listener_port: port,
        };
        
        // Start the FIX engine
        handler.start().await?;
        
        Ok(handler)
    }
    
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("Starting FIX engine on port {}", self.listener_port);
        
        // Start TCP listener
        let addr = SocketAddr::from(([127, 0, 0, 1], self.listener_port));
        let listener = TcpListener::bind(addr).await?;
        info!("FIX listener bound to {}", addr);
        
        // Start connection handler
        let session = self.session.clone();
        let inbound_tx = self.inbound_tx.clone();
        let outbound_tx = self.outbound_tx.clone();
        
        tokio::spawn(async move {
            if let Err(e) = Self::handle_connections(listener, session, inbound_tx, outbound_tx).await {
                error!("FIX connection handler error: {}", e);
            }
        });
        
        // Start message processor
        if let Some(message_rx) = self.message_rx.write().await.take() {
            let session_clone = self.session.clone();
            let outbound_tx_clone = self.outbound_tx.clone();
            
            tokio::spawn(async move {
                Self::process_outbound_messages(message_rx, session_clone, outbound_tx_clone).await;
            });
        }
        
        // Start heartbeat timer
        let session_heartbeat = self.session.clone();
        let outbound_tx_heartbeat = self.outbound_tx.clone();
        tokio::spawn(async move {
            Self::heartbeat_timer(session_heartbeat, outbound_tx_heartbeat).await;
        });
        
        Ok(())
    }
    
    #[instrument(skip(listener, inbound_tx, outbound_tx))]
    async fn handle_connections(
        listener: TcpListener,
        session: FixSession,
        inbound_tx: broadcast::Sender<ParsedFixMessage>,
        outbound_tx: mpsc::UnboundedSender<String>,
    ) -> Result<()> {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New FIX connection from {}", addr);
                    let session_clone = session.clone();
                    let inbound_tx_clone = inbound_tx.clone();
                    let outbound_tx_clone = outbound_tx.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            stream, 
                            session_clone, 
                            inbound_tx_clone, 
                            outbound_tx_clone
                        ).await {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    
    #[instrument(skip(stream, inbound_tx, outbound_tx))]
    async fn handle_connection(
        stream: TcpStream,
        session: FixSession,
        inbound_tx: broadcast::Sender<ParsedFixMessage>,
        outbound_tx: mpsc::UnboundedSender<String>,
    ) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut buf_reader = BufReader::new(reader);
        let mut outbound_rx = {
            let (tx, rx) = mpsc::unbounded_channel();
            tx
        };
        
        // Start outbound message sender
        let writer_session = session.clone();
        let send_task = tokio::spawn(async move {
            let mut outbound_messages = outbound_tx.subscribe().unwrap_or_else(|_| {
                let (_, rx) = broadcast::channel(1);
                rx
            });
            
            loop {
                select! {
                    msg = outbound_messages.recv() => {
                        match msg {
                            Ok(message) => {
                                debug!("Sending FIX message: {}", message);
                                if let Err(e) = writer.write_all(message.as_bytes()).await {
                                    error!("Failed to write message: {}", e);
                                    break;
                                }
                                if let Err(e) = writer.flush().await {
                                    error!("Failed to flush: {}", e);
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Heartbeat check
                        if !*writer_session.is_logged_on.read().await {
                            continue;
                        }
                        
                        let last_heartbeat = *writer_session.last_heartbeat.read().await;
                        if last_heartbeat.elapsed() > writer_session.heartbeat_interval * 2 {
                            warn!("Heartbeat timeout, disconnecting");
                            break;
                        }
                    }
                }
            }
        });
        
        // Start inbound message reader
        let reader_session = session.clone();
        let read_task = tokio::spawn(async move {
            let mut line = String::new();
            
            loop {
                line.clear();
                match timeout(Duration::from_secs(60), buf_reader.read_line(&mut line)).await {
                    Ok(Ok(0)) => {
                        info!("Connection closed by peer");
                        break;
                    }
                    Ok(Ok(_)) => {
                        if let Some(parsed) = Self::parse_fix_message(&line, &reader_session) {
                            debug!("Received FIX message: {}", parsed.msg_type);
                            
                            // Update sequence number
                            reader_session.seq_num_in.store(parsed.seq_num + 1, Ordering::Relaxed);
                            
                            // Handle specific message types
                            match parsed.msg_type.as_str() {
                                MSG_TYPE_LOGON => {
                                    info!("Received logon message");
                                    *reader_session.is_logged_on.write().await = true;
                                    *reader_session.last_heartbeat.write().await = Instant::now();
                                }
                                MSG_TYPE_HEARTBEAT => {
                                    debug!("Received heartbeat");
                                    *reader_session.last_heartbeat.write().await = Instant::now();
                                }
                                MSG_TYPE_LOGOUT => {
                                    info!("Received logout message");
                                    *reader_session.is_logged_on.write().await = false;
                                    break;
                                }
                                _ => {
                                    // Forward other messages
                                    if let Err(e) = inbound_tx.send(parsed) {
                                        error!("Failed to forward inbound message: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Read error: {}", e);
                        break;
                    }
                    Err(_) => {
                        error!("Read timeout");
                        break;
                    }
                }
            }
        });
        
        // Wait for either task to complete
        select! {
            _ = send_task => {},
            _ = read_task => {},
        }
        
        info!("FIX connection closed");
        Ok(())
    }
    
    #[instrument(skip(message_rx, outbound_tx))]
    async fn process_outbound_messages(
        mut message_rx: mpsc::UnboundedReceiver<FixMessage>,
        session: FixSession,
        outbound_tx: mpsc::UnboundedSender<String>,
    ) {
        while let Some(message) = message_rx.recv().await {
            debug!("Processing outbound message: {:?}", message);
            
            if let Ok(fix_message) = Self::build_fix_message(&message, &session) {
                if let Err(e) = outbound_tx.send(fix_message) {
                    error!("Failed to send outbound message: {}", e);
                    break;
                }
            }
        }
    }
    
    #[instrument(skip(outbound_tx))]
    async fn heartbeat_timer(
        session: FixSession,
        outbound_tx: mpsc::UnboundedSender<String>,
    ) {
        let mut heartbeat_interval = interval(session.heartbeat_interval);
        
        loop {
            heartbeat_interval.tick().await;
            
            if !*session.is_logged_on.read().await {
                continue;
            }
            
            // Send heartbeat
            let heartbeat = Self::build_heartbeat(&session);
            if let Err(e) = outbound_tx.send(heartbeat) {
                error!("Failed to send heartbeat: {}", e);
                break;
            }
            
            debug!("Sent heartbeat");
        }
    }
    
    fn parse_fix_message(message: &str, session: &FixSession) -> Option<ParsedFixMessage> {
        let mut fields = HashMap::new();
        let parts: Vec<&str> = message.trim().split(SOH).collect();
        
        for part in parts {
            if let Some((tag_str, value)) = part.split_once('=') {
                if let Ok(tag) = tag_str.parse::<u16>() {
                    fields.insert(tag, value.to_string());
                }
            }
        }
        
        // Extract required fields
        let msg_type = fields.get(&35)?.clone(); // MsgType
        let seq_num = fields.get(&34)?.parse().ok()?; // MsgSeqNum
        let sender_comp_id = fields.get(&49)?.clone(); // SenderCompID
        let target_comp_id = fields.get(&56)?.clone(); // TargetCompID
        let sending_time_str = fields.get(&52)?; // SendingTime
        
        let sending_time = DateTime::parse_from_str(sending_time_str, "%Y%m%d-%H:%M:%S")
            .ok()?
            .with_timezone(&Utc);
        
        Some(ParsedFixMessage {
            fields,
            msg_type,
            seq_num,
            sender_comp_id,
            target_comp_id,
            sending_time,
        })
    }
    
    fn build_fix_message(message: &FixMessage, session: &FixSession) -> Result<String> {
        let seq_num = session.seq_num_out.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S").to_string();
        
        let mut fields = Vec::new();
        
        // Standard header
        fields.push(format!("8={}", FIX_VERSION)); // BeginString
        fields.push(format!("35={}", message.msg_type)); // MsgType
        fields.push(format!("34={}", seq_num)); // MsgSeqNum
        fields.push(format!("49={}", session.sender_comp_id)); // SenderCompID
        fields.push(format!("56={}", session.target_comp_id)); // TargetCompID
        fields.push(format!("52={}", sending_time)); // SendingTime
        
        // Message-specific fields
        match message.msg_type.as_str() {
            MSG_TYPE_NEW_ORDER_SINGLE => {
                fields.push(format!("11={}", message.order_id)); // ClOrdID
                fields.push(format!("55={}", message.symbol)); // Symbol
                fields.push(format!("54={}", message.side)); // Side
                fields.push(format!("38={}", message.quantity)); // OrderQty
                fields.push(format!("40={}", message.order_type)); // OrdType
                
                if let Some(price) = message.price {
                    fields.push(format!("44={}", price)); // Price
                }
                
                fields.push("59=0".to_string()); // TimeInForce = Day
            }
            _ => {
                return Err(anyhow!("Unsupported message type: {}", message.msg_type));
            }
        }
        
        // Calculate body length (excluding BeginString and BodyLength fields)
        let body = fields[1..].join(&SOH.to_string());
        let body_length = body.len() + SOH.to_string().len(); // +1 for trailing SOH
        
        // Insert body length after BeginString
        fields.insert(1, format!("9={}", body_length));
        
        // Calculate checksum
        let message_without_checksum = fields.join(&SOH.to_string()) + &SOH.to_string();
        let checksum = Self::calculate_checksum(&message_without_checksum);
        fields.push(format!("10={:03}", checksum));
        
        let final_message = fields.join(&SOH.to_string()) + &SOH.to_string();
        Ok(final_message)
    }
    
    fn build_heartbeat(session: &FixSession) -> String {
        let seq_num = session.seq_num_out.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S").to_string();
        
        let mut fields = Vec::new();
        fields.push(format!("8={}", FIX_VERSION));
        fields.push(format!("35={}", MSG_TYPE_HEARTBEAT));
        fields.push(format!("34={}", seq_num));
        fields.push(format!("49={}", session.sender_comp_id));
        fields.push(format!("56={}", session.target_comp_id));
        fields.push(format!("52={}", sending_time));
        
        let body = fields[1..].join(&SOH.to_string());
        let body_length = body.len() + SOH.to_string().len();
        fields.insert(1, format!("9={}", body_length));
        
        let message_without_checksum = fields.join(&SOH.to_string()) + &SOH.to_string();
        let checksum = Self::calculate_checksum(&message_without_checksum);
        fields.push(format!("10={:03}", checksum));
        
        fields.join(&SOH.to_string()) + &SOH.to_string()
    }
    
    fn calculate_checksum(message: &str) -> u8 {
        message.bytes().fold(0u8, |acc, byte| acc.wrapping_add(byte)) % 256
    }
    
    #[instrument(skip(self))]
    pub async fn send_message(&self, message: FixMessage) -> Result<()> {
        self.message_tx.send(message)?;
        Ok(())
    }
    
    pub fn subscribe_to_messages(&self) -> broadcast::Receiver<ParsedFixMessage> {
        self.inbound_tx.subscribe()
    }
    
    #[instrument(skip(self))]
    pub async fn send_logon(&self) -> Result<()> {
        let logon = self.build_logon_message()?;
        self.outbound_tx.send(logon)?;
        Ok(())
    }
    
    fn build_logon_message(&self) -> Result<String> {
        let seq_num = self.session.seq_num_out.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S").to_string();
        
        let mut fields = Vec::new();
        fields.push(format!("8={}", FIX_VERSION));
        fields.push(format!("35={}", MSG_TYPE_LOGON));
        fields.push(format!("34={}", seq_num));
        fields.push(format!("49={}", self.session.sender_comp_id));
        fields.push(format!("56={}", self.session.target_comp_id));
        fields.push(format!("52={}", sending_time));
        fields.push(format!("108={}", self.session.heartbeat_interval.as_secs())); // HeartBtInt
        fields.push("98=0".to_string()); // EncryptMethod = None
        
        let body = fields[1..].join(&SOH.to_string());
        let body_length = body.len() + SOH.to_string().len();
        fields.insert(1, format!("9={}", body_length));
        
        let message_without_checksum = fields.join(&SOH.to_string()) + &SOH.to_string();
        let checksum = Self::calculate_checksum(&message_without_checksum);
        fields.push(format!("10={:03}", checksum));
        
        Ok(fields.join(&SOH.to_string()) + &SOH.to_string())
    }
    
    #[instrument(skip(self))]
    pub async fn send_logout(&self) -> Result<()> {
        let logout = self.build_logout_message()?;
        self.outbound_tx.send(logout)?;
        
        // Update session state
        *self.session.is_logged_on.write().await = false;
        Ok(())
    }
    
    fn build_logout_message(&self) -> Result<String> {
        let seq_num = self.session.seq_num_out.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S").to_string();
        
        let mut fields = Vec::new();
        fields.push(format!("8={}", FIX_VERSION));
        fields.push(format!("35={}", MSG_TYPE_LOGOUT));
        fields.push(format!("34={}", seq_num));
        fields.push(format!("49={}", self.session.sender_comp_id));
        fields.push(format!("56={}", self.session.target_comp_id));
        fields.push(format!("52={}", sending_time));
        
        let body = fields[1..].join(&SOH.to_string());
        let body_length = body.len() + SOH.to_string().len();
        fields.insert(1, format!("9={}", body_length));
        
        let message_without_checksum = fields.join(&SOH.to_string()) + &SOH.to_string();
        let checksum = Self::calculate_checksum(&message_without_checksum);
        fields.push(format!("10={:03}", checksum));
        
        Ok(fields.join(&SOH.to_string()) + &SOH.to_string())
    }
    
    pub async fn is_logged_on(&self) -> bool {
        *self.session.is_logged_on.read().await
    }
    
    pub fn get_sequence_numbers(&self) -> (u64, u64) {
        (
            self.session.seq_num_out.load(Ordering::Relaxed),
            self.session.seq_num_in.load(Ordering::Relaxed),
        )
    }
}

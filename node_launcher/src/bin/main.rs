use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead};
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// Import the Node struct from the "node" library
use node::Node; // Assumes that Node is defined in the crate "node"

/// Main entry point to start a node in the distributed system.
///
/// This program is used to initialize a node in a network of distributed nodes
/// that communicate with each other. The node is initialized with a given IP address,
/// provided as a command-line argument, and seed IPs are read from a `seed_nodes.txt` file.
///
/// # Usage
///
/// Run the program by providing the node IP as the only argument:
///
/// ```sh
/// cargo run -- <node_ip>
/// ```
///
/// # Example Execution
///
/// ```sh
/// cargo run -- 192.168.1.2
/// ```
///
/// # Errors
///
/// The program returns an error if:
/// - The number of arguments is incorrect.
/// - The provided IP address is invalid.
/// - The seed_nodes.txt file does not exist or cannot be read.
///
/// # Return Values
///
/// - `Ok(())` - The node started successfully.
/// - `Err(String)` - There was an error starting the node.
fn main() -> Result<(), String> {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check that exactly one argument (node IP) is provided
    if args.len() != 2 {
        return Err("Usage: program <node_ip>".to_string());
    }

    // Pause for a brief moment before continuing, allowing other nodes to initialize
    thread::sleep(Duration::from_millis(200));

    // Parse the provided node IP address
    let node_ip = Ipv4Addr::from_str(&args[1]).map_err(|_| "Invalid IP address".to_string())?;

    // Read seed node IPs from the seed_nodes.txt file
    let seed_ips = read_seed_ips("seed_nodes.txt")?;

    // Create the node with the specified IP and the list of seed IPs
    let node = Arc::new(Mutex::new(
        Node::new(node_ip, seed_ips).map_err(|e| e.to_string())?,
    ));

    // Initialize the connections map
    let connections = Arc::new(Mutex::new(HashMap::new()));

    // Start the node with the specified IP and connection map
    Node::start(Arc::clone(&node), Arc::clone(&connections)).map_err(|e| e.to_string())?;

    Ok(())
}

/// Reads seed IP addresses from a file and returns them as a vector of `Ipv4Addr`.
///
/// This function expects a file named `seed_nodes.txt` in the current directory,
/// with each line containing a single IP address. IP addresses must be valid IPv4 addresses.
///
/// # Arguments
///
/// * `file_path` - The path to the seed nodes file.
///
/// # Returns
///
/// A `Result` containing:
/// - `Ok(Vec<Ipv4Addr>)` - A vector of seed IP addresses on success.
/// - `Err(String)` - An error message if the file could not be read or if any IP is invalid.
fn read_seed_ips(file_path: &str) -> Result<Vec<Ipv4Addr>, String> {
    // Attempt to open the file
    let file = File::open(file_path).map_err(|_| format!("Failed to open {}", file_path))?;

    // Create a buffer to read the file line by line
    let reader = io::BufReader::new(file);

    // Process each line and parse it as an IPv4 address
    let mut seed_ips = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|_| "Error reading seed IPs")?;
        let ip = Ipv4Addr::from_str(&line)
            .map_err(|_| format!("Invalid IP in {}: {}", file_path, line))?;
        seed_ips.push(ip);
    }

    // Return the vector of parsed IP addresses
    Ok(seed_ips)
}

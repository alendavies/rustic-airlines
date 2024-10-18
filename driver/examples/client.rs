use std::{net::Ipv4Addr, str::FromStr};

use driver::CassandraClient;

fn main() {
    // Replace with the correct IP address and port of the server
    let server_ip = "127.0.0.1";
    let ip = Ipv4Addr::from_str(&server_ip).unwrap();

    let mut client = CassandraClient::connect(ip).unwrap();
    client.startup().unwrap();

    let query = "CREATE KEYSPACE world WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";
    let res = client.execute(&query).unwrap();

    dbg!(&res);
}

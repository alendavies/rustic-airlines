# Taller de Programacion - Grupo "Ferrum"

![GUI](https://github.com/user-attachments/assets/7e9b9e5e-4c07-437f-9e3b-480523320984)

## Integrantes

- Lorenzo Minervino: 107863
- Federico Camurri: 106359
- Alen Davies: 107084
- Luca Lazcano: 107044

## Introducción

Este trabajo práctico consiste en la implementación de un sistema de control de vuelos global. Para ello es necesaria la implementación de una base de datos distribuida del estilo Cassandra, que permita el acceso para lectura y escritura por múltiples clientes distribuidos.

## Como usar

A continuacion se detallan los pasos para compilar y ejecutar el programa.

## Compilacion y ejecucion

Para compilar y ejecutar el programa existen las siguientes opciones:

-   Levantar 5 nodos automaticamente: Correr `make run` en el directorio `node_launcher`.
-   Levantar cada nodo individualmente: Correr `cargo run {ip}` en el directorio `node_launcher`.

-   Para crear tablas y keyspaces y cargar datos de prueba, correr `cargo run` en `driver/examples`
-   Para crear tablas y keyspaces y cargar datos de vuelos y aeropuertos para la interfaz gráfica correr `cargo run --example airports` en `graphical-interface`.
-   Para ejecutar la interfaz gráfica, en base a los datos previamente cargados al cluster, ejecutar `cargo run` en `graphical-interface`.

## Como testear

## Correr con Docker

Para correr el clúster de nodos definido en `compose.yml` con: `sudo docker compose --profile "*" up --build`.

El driver intenta utilizar la variable de entorno `NODE_ADDR` para conectarse con el cluster. Por ejemplo, corriendo `export NODE_ADDR="127.0.0.1:10000" && cargo run` en `graphical-interface`, la interfaz gŕafica se conecta al cluster por el puerto 10000 mapeado a un nodo del cluster seún lo definido en `compose.yml`.

- `cd graphical-interface && export NODE_ADDR="127.0.0.1:10001" && cargo run`

- `cd flight-sim && export NODE_ADDR="127.0.0.1:10002" && cargo run`

La IP nodo semilla utilizado por un nodo también puede configurarse seteando la variable de entorno `SEED`.

Se pueden agregar y quitar nodos configurando correctamente las direcciones IP en `compose.yml`.

Para correr el perfil `initial-nodes` de Compose, correr `sudo docker compose --profile initial-nodes up`. De este modo se levantan los nodos incluidos en dicho perfile. Luego se puede correr un nuevo nodo, para simular la unión al cluster, con `sudo docker compose --profile new-node up`. Se pueden agregar más perfiles con nodos para simular la unión de más nodos.

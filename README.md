# Taller de Programacion - Grupo "Ferrum"

## Integrantes

Lorenzo Minervino: 107863
Federico Camurri: 106359
Alen Davies: 107084
Luca Lazcano: 107044

## Introducción

Este trabajo práctico consiste en la implementación de un sistema de control de vuelos global. Para ello es necesarios la implementación

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

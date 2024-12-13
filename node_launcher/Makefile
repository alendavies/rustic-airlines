IP1 = 127.0.0.1
IP2 = 127.0.0.2
IP3 = 127.0.0.3
IP4 = 127.0.0.4
IP5 = 127.0.0.5

run:
	gnome-terminal -- bash -c "cargo run -- $(IP1) /home/lminervino18/Escritorio; exec bash" &
	sleep 2
	gnome-terminal -- bash -c "cargo run -- $(IP2) /home/lminervino18/Escritorio; exec bash" &
	gnome-terminal -- bash -c "cargo run -- $(IP3) /home/lminervino18/Escritorio; exec bash" &
	gnome-terminal -- bash -c "cargo run -- $(IP4) /home/lminervino18/Escritorio; exec bash" &
	gnome-terminal -- bash -c "cargo run -- $(IP5) /home/lminervino18/Escritorio; exec bash" &

.PHONY: run

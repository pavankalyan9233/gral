all: debug

release: Makefile
	cargo build --release

debug: Makefile
	cargo build

docker: Makefile Dockerfile
	docker build -t neunhoef/gral:0.1.0 .
	docker push neunhoef/gral:0.1.0

clean:
	rm -rf target tls

keys: Makefile makekeys.sh
	./makekeys.sh

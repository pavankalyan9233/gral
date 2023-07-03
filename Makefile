all: release

release: Makefile
	cargo build --release

debug: Makefile
	cargo build

docker: Makefile Dockerfile
	docker build -t neunhoef/gral .
	docker push neunhoef/gral

clean:
	rm -rf target

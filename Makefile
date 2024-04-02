all: debug

release: Makefile
	cargo build --release

debug: Makefile
	cargo build

docker: Makefile Dockerfile
	docker build -t gcr.io/gcr-for-testing/neunhoef/gral:0.1.0 .
	docker push gcr.io/gcr-for-testing/neunhoef/gral:0.1.0

# For those who do not have a local `protoc-gen-doc` installation available, but docker:
docker-apidoc: Makefile
	docker run --rm --platform linux/amd64 \
      -v ./protodoc:/out \
      -v ./proto:/protos \
      pseudomuto/protoc-gen-doc --doc_opt=out/ourhtml.tmpl,graphanalytics.html protos/graphanalyticsengine.proto

apidoc: Makefile proto/graphanalyticsengine.proto protodoc/ourhtml.tmpl
	protoc -I proto --doc_out=protodoc --doc_opt=protodoc/ourhtml.tmpl,graphanalytics.html proto/graphanalyticsengine.proto

clean:
	rm -rf target tls

keys: Makefile makekeys.sh
	./makekeys.sh

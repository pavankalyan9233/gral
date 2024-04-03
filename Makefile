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
      pseudomuto/protoc-gen-doc --doc_opt=out/ourhtml.mustache,graphanalytics.html protos/graphanalyticsengine.proto && \
      jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < ./protodoc/setup.md | curl --data @- https://api.github.com/markdown > ./protodoc/setup.html && \
      sed -i -e '/<!--INSERTHERE-->/r protodoc/setup.html' protodoc/graphanalytics.html

docker-apidoc-pdf: Makefile
	docker run --rm --platform linux/amd64 -v "./protodoc:/workspace" pink33n/html-to-pdf --url http://localhost/graphanalytics.html --pdf /workspace/graphanalytics.pdf ; \
	exit 0

apidoc: Makefile proto/graphanalyticsengine.proto protodoc/ourhtml.mustache
	protoc -I proto --doc_out=protodoc --doc_opt=protodoc/ourhtml.mustache,graphanalytics.html proto/graphanalyticsengine.proto && \
	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < ./protodoc/setup.md | curl --data @- https://api.github.com/markdown > ./protodoc/setup.html && \
	sed -i -e '/<!--INSERTHERE-->/r protodoc/setup.html' protodoc/graphanalytics.html

clean:
	rm -rf target tls

keys: Makefile makekeys.sh
	./makekeys.sh

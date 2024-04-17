# Setting up Graph Analytics Engines (GAEs) on ArangoGraph

It is now possible to deploy GAEs (graph analytics engines) on ArangoGraph.
Note that currently this is behind a feature flag and has to be activated
by an employee of ArangoDB for your ArangoGraph organization. Note that every
GAE is attached to one particular ArangoDB database deployment on ArangoGraph.

Once the feature flag is activated, you can follow the instructions below.

## Deploying and undeploying GAEs

You can deploy and undeploy GAEs on an existing deployment via API calls to ArangoGraph that are authenticated by a valid ArangoGraph access token.

### <a name="platform_authentication"></a> Authentication

Authentication is done via an ArangoGraph access token. See the [ArangoGraph Documentation](https://docs.arangodb.com/stable/arangograph/api/get-started/#authenticating-with-oasisctl) on how to create this. Let's save this token in a variable
```
export ARANGO_GRAPH_TOKEN
```
Then, the API calls for deploying and undeploying GAEs require the following header:
```
Authorization: bearer $ARANGO_GRAPH_TOKEN
```

### Base URL

The base url consists of the deployment url, a special engine port and a constant path. If your database deployment is available under the endpoint `$DEPLOYMENT_URL:$PORT`, e.g. `https://62535a263232.arangodb.cloud:8529`
then you need to access the ArangoGraph API under the URL
```
export BASE_URL=$DEPLOYMENT_URL:8829/graph-analytics/api/graphanalytics/v1
```
e.g. `https://62535a263232.arangodb.cloud:8829/graph-analytics/api/graphanalytics/v1`

### <a name="deploy_api"></a> API

The following API calls are available, all start with the `BASE_URL` from above:

 - `GET $BASE_URL/api-version`: return a JSON document describing the API version
 - `GET $BASE_URL/enginetypes`: return a JSON document describing the available
   GAE types, currently, there is only one called `gral` available
 - `GET $BASE_URL/enginesizes`: return a JSON document describing the available
   GAE sizes, currently, there are a certain number of choices available,
   which basically choose the number of cores and the size of the RAM
 - `GET $BASE_URL/engines`: return a list of currently deployed GAEs for this
   database deployment
 - `GET $BASE_URL/engine/<id>`: return information about a specific GAE
 - `POST $BASE_URL/engines` with a body like this:

```json
{"type_id":"gral", "size_id": "e32"}'
```

   This will deploy an engine of type `gral` with size `e32`, which means
   32 GB of RAM and 8 cores.

 - `DELETE $BASE_URL/engine/<id>`: undeploy (delete) a specific GAE

All these API calls can be executed conveniently with the provided `gae` shell script. Just set the environment variables `ARANGO_GRAPH_TOKEN` to the access token described above and `DEPLOYMENT_URL` to the URL of your deployment (leaving out the port part :8529). Instead of `ARANGO_GRAPH_TOKEN` you can also set the variables `ARANGO_GRAPH_API_KEY_ID` and `ARANGO_GRAPH_API_KEY_SECRET` to your API key ID and secret, the `gae` script will then automatically get an `ARANGO_GRAPH_TOKEN`.

## Interacting with a running GAE

You can access a running GAE via API calls to ArangoGraph which are forwarded to the respective GAE. Here the authentication depends on the database deployment mode.

### Authentication

In ArangoGraph there are two modes, in which your database deployment can be:

 1. Platform authentication switched on.
 2. Platform authentication switched off.

The authentication for API calls to the GAE works differently for these two
cases.

In Case 1. you have to give the exact same authorization header [as
above](#platform_authentication) with an ArangoGraph access token. The platform will automatically
verify the validity of the token and, if authenticated, will change
the authorization header on the fly to provide one with a JWT token,
which the GAE can use to access the database deployment! This means
in particular, that the GAE will have the access permissions of the
ArangoGraph platform user, which also exists as an ArangoDB user in the
database deployment!

In Case 2. you have to directly provide a valid JWT token in the
`Authorization` header, for some user which is configured in the
database deployment. See the [ArangoDB Documentation](https://docs.arangodb.com/3.11/develop/http-api/authentication/#jwt-user-tokens) on how to aquire it. Then, the authorization header for the API calls need to be
```
Authorization: bearer $JWT_TOKEN
```

### Base URL

To access a running engine, you need the engine url. You get this url via the `GET $BASE_URL/engine/<id>` ArangoGraph request described in the [deployment api](#deploy_api), under `status` and `endpoint` entry. Let's call this
```
export ENGINE_URL
```

### API

The API of the GAE is documented below. The documentation is automatically generated from the protobuf API description, so that the bodies of the HTTP requests are documented in detail.


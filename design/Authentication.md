# Authentication

We have decided that we want to do authentication solely by JWT tokens.
A user will have to authenticate somehow with the database and get a
JWT token for some user (or the superuser with empty user) from there.

This means that in the GAE, we have very little to do:

 1. The engine needs access to the shared secret of the cluster. This will
    typically be done exactly as for `arangod`: There is a directory with
    valid secrets, of which one is marked special to be the signing secret.
    A JWT token is accepted, if it is signed by any of the secrets given
    and new JWT tokens are created by signing them with the special one.

 2. Every incoming request has to have an HTTP header "Authentication" whose
    value is `"Bearer "` followed by a single JWT token. The payload of the
    token contains a `preferred_user` entry which is the ArangoDB user name
    for which the token was created and signed. Every request without a valid
    token is rejected with HTTP 401. Every successful authentication gets
    a username from the token (which can be empty for superuser).

 3. Every request we send to the database must use a JWT token which contains
    the username which was given in the HTTP request which triggered the action.
    This allows proper authentication with the database.



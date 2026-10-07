# Runbook: Certificate rotation (D5)

Symptom: listener certificates approach or pass expiry; handshakes start failing.

## Detect
- Alert `CertExpiringSoon` (warning, < 14 days) fed by `scripts/cert-expiry-check.sh` via Pushgateway.
- Alert `CertExpired` (critical): clients log SSL handshake failures.

## Diagnose
- `bash security/scripts/rotate-certs.sh --check` - remaining validity per certificate.

## Mitigate (rotation, no downtime)
1. Re-issue the certificate from the same CA:
   - broker: `bash security/scripts/rotate-certs.sh --type broker --name broker-1`
   - client: `bash security/scripts/rotate-certs.sh --type client --name svc-demo`
2. Kafka reloads file-based PKCS12 keystores without a restart (SSL listener config points at the mounted file).
3. Re-run `--check` and `scripts/cert-expiry-check.sh`; alerts clear on the next push.

## Recover (already expired)
- Expired certificates are refused at the TLS layer (verified locally with `openssl verify -attime`; the cluster-level refusal is the D5 drill evidence).
- Rotate as above; restarting the affected client/broker container is the fallback if a cached handshake lingers.

## Prevent
- Default broker/client validity is 30 days on purpose: rotation is routine, not an emergency.
- `make cert-expiry-check` in the drill cadence; nightly push in CI when hardware allows.

#!/usr/bin/env bash
# Job body: open a transfer, run CrystFEL against it, ship the stream.
#
#   crystfel_nersc.sh <exp=...,run=...> <token path> [indexamajig args...]
#
# The caller's arguments replace the defaults below. We still own the
# input URI, geometry, and output paths, which only exist here.
#
# Two images: ours only talks to lclstream_api, CrystFEL's is upstream and
# unmodified, so its arguments and geometry change without a rebuild.
set -uo pipefail

# Slurm starts us in the job dir, which the submit script sets.
JOB_DIR=$PWD

# Slurm inherits nothing, so the xrootd settings come from here.
set -a
[[ -f .env ]] && . ./.env
set +a

export LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER=${1:?"expected exp=<exp>,run=<run>"}
TOKEN_FILE=${2:?"expected the staged token path"}
shift 2
# Defaults only; the caller's string replaces all of them.
CRYSTFEL_ARGS=("$@")
(( $# )) || CRYSTFEL_ARGS=(
    --zmq-request=next
    --data-format=msgpack
    --peaks=msgpack
    --copy-header=timestamp
    --copy-header=event_id
    --copy-header=source
)
[[ -r "${TOKEN_FILE}" ]] || { echo "[token] not readable: ${TOKEN_FILE}"; exit 1; }
echo "[source] ${LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER}"

XRD_DEST=${XRD_DEST:-}
export XrdSecSSSKT=${XrdSecSSSKT:-/global/homes/p/psdatmgr/.xrd/sss_iri.keytab}
if [[ -n "${XRD_DEST}" && ! -r "${XrdSecSSSKT}" ]]; then
    echo "[xrootd] no keytab: ${XrdSecSSSKT}"
    exit 1
fi
XRD_URL=${XRD_URL:-root://sdfdtn006.slac.stanford.edu:3091}
export XRD_NETWORKSTACK=IPv4

SCRATCH_DIR=${PSCRATCH:-/pscratch/sd/p/psdatmgr}/crystfel-demo
# exp=<exp>,run=<run> -> "<exp>_<run>", so a stream says which data it holds
# and the job id keeps two runs of one exp/run apart.
SOURCE_SLUG=${LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER#exp=}
SOURCE_SLUG=${SOURCE_SLUG/,run=/_}
STREAM_NAME=crystfel.${SOURCE_SLUG}.${SLURM_JOB_ID:-manual}.stream
STREAM=${SCRATCH_DIR}/${STREAM_NAME}
mkdir -p "${SCRATCH_DIR}" || exit 1

# EJFat
echo "EJFAT_URI used: ${EJFAT_URI}"
echo "EJFAT Status:"
podman-hpc run --rm --network host -e EJFAT_URI=${EJFAT_URI} localhost/ejfat_e2sar:1.0 lbadm --status

# Start local cache (nersc)
echo "Starting NERSC cache"
CACHE_NAME=fastcache-${SLURM_JOB_ID:-$$}
podman-hpc run --rm --name "${CACHE_NAME}" \
        -e EJFAT_URI=${EJFAT_URI} \
        --network host \
        -v /global/homes/p/psdatmgr/repos/github/lclstream/lclstream_api/client/example/default.json:/usr/local/bin/default.json localhost/fastcache_ejfat:1.0 /usr/local/bin/lclstream-fastcache /usr/local/bin/default.json &
CACHE_PID=$!
echo "[cache] Waiting for ZMQ port to become active..."
for i in {1..30}; do
    if nc -z 127.0.0.1 12120 2>/dev/null; then
        echo "[cache] Port 12120 is open!"
        break
    fi
    sleep 1
done

echo "[crystfel] args: ${CRYSTFEL_ARGS[*]}"

CLIENT_IMAGE=${CLIENT_IMAGE:-lclstream-client-v2:3.0}
CRYSTFEL_IMAGE=${CRYSTFEL_IMAGE:-gitlab.desy.de:5555/thomas.white/crystfel/crystfel:latest}
GEOM=${CRYSTFEL_GEOM:-jf16mgeom.geom}

client() {
    podman-hpc run --rm --network=host \
	-e EJFAT_URI \
        -v "${JOB_DIR}:/cfg:ro" \
        -v "${TOKEN_FILE}:/run/s3df_token:ro" \
        -e LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER \
        "${CLIENT_IMAGE}" "$@"
}

# Slurm SIGTERMs us at the wall clock, and bash skips EXIT traps on an
# untrapped signal, so catch both or a timeout leaks the transfer.
cleanup() {
    trap - EXIT TERM INT
    [[ -n "${CRYSTFEL_PID:-}" ]] && kill "${CRYSTFEL_PID}" 2>/dev/null
    [[ -n "${TRANSFER_ID:-}" ]] && client cancel "${TRANSFER_ID}"
    [[ -n "${CACHE_PID:-}" ]] && podman-hpc rm -f "${CACHE_NAME}" >/dev/null 2>&1
    rm -f "${TOKEN_FILE}"
}
trap cleanup EXIT
trap 'cleanup; exit 143' TERM INT

echo "[transfer] Creating data transfer session..."
client create &
#opened=$(client create | tee /dev/stderr) || { echo "[transfer] create failed"; exit 1; }
#TRANSFER_ID=$(sed -n 's/^TRANSFER_ID=//p' <<<"${opened}")

#if [[ -z "${TRANSFER_ID}" ]]; then
#    echo "[error] Failed to parse TRANSFER_ID from client create output." >&2
#    exit 1
#fi
#echo "[transfer] Successfully acquired TRANSFER_ID: ${TRANSFER_ID}"

# Backgrounded on purpose: bash defers traps while a foreground child
# runs, so a wall-clock SIGTERM would never reach cleanup.
rc=0
echo "[crystfel] Launching indexamajig..."
podman-hpc run --rm --network=host \
    -v "${JOB_DIR}:/cfg:ro" \
    -v "${SCRATCH_DIR}:/out" \
    "${CRYSTFEL_IMAGE}" \
    indexamajig \
        --zmq-input="tcp://127.0.0.1:12120" \
        "${CRYSTFEL_ARGS[@]}" \
        --temp-dir=/out \
        -o "/out/${STREAM_NAME}" &
CRYSTFEL_PID=$!
wait "${CRYSTFEL_PID}" || rc=$?
CRYSTFEL_PID=
echo "[crystfel] exit ${rc}"

# Ship whatever indexed, even on failure.
export XrdSecSSSKT=${HOME}/.xrd/sss_lstream.keytab
if [[ -z "${XRD_DEST}" ]]; then
    echo "[xrootd] XRD_DEST unset; stream kept at ${STREAM}"
elif [[ -s "${STREAM}" ]]; then
    dest="${XRD_URL}/${XRD_DEST%/}/${STREAM_NAME}"
    echo "[xrootd] copying ${STREAM} to ${dest}"
    xrdcp --force "${STREAM}" "${dest}" || rc=$?
else
    echo "[xrootd] no stream to copy"
fi

echo "[done] exit ${rc}"
exit "${rc}"

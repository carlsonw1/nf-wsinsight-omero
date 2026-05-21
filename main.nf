/*
  Minimal DSL2 pipeline: one process that
  1) refreshes AWS credentials inside the wsinsight container via samlutil (pexpect)
  2) runs wsinsight against an S3 input URI and writes results locally

  Required CLI args:
    --wsi_dir       (s3://... or local path)
    --outdir        (host path mounted into container; e.g. /workspace/wsinsight_results)

  Required env on host (must be passed into container via nextflow.config envWhitelist):
    SAML_USERNAME
    SAML_PASSWORD
  Optional:
    SAML_OTP_CMD    (shell cmd that prints OTP)
*/

nextflow.enable.dsl=2

def dotenv = [:]

def envFile = new File("${projectDir}/.env")
if (envFile.exists()) {
    envFile.eachLine { rawLine ->
        def line = rawLine.trim()

        if (!line || line.startsWith('#')) return

        line = line.replaceFirst(/^export\s+/, '')

        if (!line.contains('=')) return

        def parts = line.split('=', 2)
        def key = parts[0].trim()
        def val = parts[1].trim()

        val = val.replaceFirst(/^['"]/, '').replaceFirst(/['"]$/, '')

        dotenv[key] = val
    }
}

def saml_username_in     = params.containsKey('saml_username') ? params.saml_username : ''
def saml_password_in     = params.containsKey('saml_password') ? params.saml_password : ''
def saml_otp_cmd_in      = params.containsKey('saml_otp_cmd')  ? params.saml_otp_cmd  : ''

def omero_host_in        = params.containsKey('omero_host') ? params.omero_host : ''
def omero_port_in        = params.containsKey('omero_port') ? params.omero_port : '4064'
def omero_username_in    = params.containsKey('omero_username') ? params.omero_username : ''
def omero_password_in    = params.containsKey('omero_password') ? params.omero_password : ''
def omero_group_in       = params.containsKey('omero_group') ? params.omero_group : ''
def omero_project_name_in= params.containsKey('omero_project_name') ? params.omero_project_name : ''
def omero_dataset_name_in= params.containsKey('omero_dataset_name') ? params.omero_dataset_name : ''

params.omero_login_timeout = params.containsKey('omero_login_timeout') ? params.omero_login_timeout : '5400'   // 90 min requested
params.omero_retry_count   = params.containsKey('omero_retry_count')   ? params.omero_retry_count   : '2'
params.omero_retry_sleep   = params.containsKey('omero_retry_sleep')   ? params.omero_retry_sleep   : '15'

params.saml_username      = saml_username_in      ?: (dotenv['SAML_USERNAME'] ?: '').trim()
params.saml_password      = saml_password_in      ?: (dotenv['SAML_PASSWORD'] ?: '').trim()
params.saml_otp_cmd       = saml_otp_cmd_in       ?: (dotenv['SAML_OTP_CMD'] ?: '').trim()

params.omero_host         = omero_host_in         ?: (dotenv['OMERO_HOST'] ?: '').trim()
params.omero_port         = omero_port_in         ?: (dotenv['OMERO_PORT'] ?: '4064').trim()
params.omero_username     = omero_username_in     ?: (dotenv['OMERO_USERNAME'] ?: '').trim()
params.omero_password     = omero_password_in     ?: (dotenv['OMERO_PASSWORD'] ?: '').trim()
params.omero_group        = omero_group_in        ?: (dotenv['OMERO_GROUP'] ?: '').trim()
params.omero_project_name = omero_project_name_in ?: (dotenv['OMERO_PROJECT_NAME'] ?: '').trim()
params.omero_dataset_name = omero_dataset_name_in ?: (dotenv['OMERO_DATASET_NAME'] ?: '').trim()

println "DEBUG env file path: ${envFile.absolutePath}"
println "DEBUG env file exists: ${envFile.exists()}"
println "DEBUG dotenv keys: ${dotenv.keySet()}"
println "DEBUG saml_username loaded: ${params.saml_username ? 'YES' : 'NO'}"
println "DEBUG saml_password loaded: ${params.saml_password ? 'YES' : 'NO'}"
println "DEBUG saml_otp_cmd loaded: ${params.saml_otp_cmd ? 'YES' : 'NO'}"
println "DEBUG OMERO_HOST loaded = [${params.omero_host}]"
println "DEBUG OMERO_PORT loaded = [${params.omero_port}]"
println "DEBUG OMERO_USER loaded = [${params.omero_username}]"
println "DEBUG OMERO_PASSWORD loaded = [${params.omero_password ? 'YES' : 'NO'}]"

workflow {
    if( !params.wsi_dir )     error "Missing --wsi_dir"
    if( !params.outdir )      error "Missing --outdir"
    if( !params.config_json ) error "Missing --config_json"

    if( !params.model && !params.model_path ) {
        error "Missing model selection: provide either --model or --model_path"
    }

    if( !params.annotation_dir ) {
        error "Missing --annotation_dir"
    }

    if( !params.omero_host )         error "Missing OMERO host"
    if( !params.omero_port )         error "Missing OMERO port"
    if( !params.omero_username )     error "Missing OMERO username"
    if( !params.omero_password )     error "Missing OMERO password"
    if( !params.omero_project_name ) error "Missing OMERO project name"
    if( !params.omero_dataset_name ) error "Missing OMERO dataset name"

    Channel.value(true).set { start_ch }

    RUN_WSINSIGHT(start_ch)

    staged = STAGE_SVS_FOR_OMERO(start_ch)
    staged.stage_done.view { "SVS staging complete" }

    svs_ch = staged.staged_svss.flatten()

    zarr_ch    = CONVERT_SVS_TO_ZARR(svs_ch).zarr_dir
    ometiff_ch = CONVERT_ZARR_TO_OMETIFF(zarr_ch).ome_tiff

    project_dataset_script_ch = Channel.value(file("${projectDir}/scripts/project_dataset.py"))
    attach_csv_to_image_script_ch = Channel.value(file("${projectDir}/scripts/attach_csv_to_image.py"))

    target_json_ch = PREPARE_OMERO_TARGET(Channel.value(true), project_dataset_script_ch).target_json

    csv_ch = Channel
        .fromPath("${params.annotation_dir}/*.ome.csv.gz", checkIfExists: true)
        .map { csv ->
            def sample_id = csv.baseName
                .replace('.ome.csv', '')
                .replace('.csv', '')
            tuple(sample_id, csv)
        }

    import_input_ch = target_json_ch
        .combine(ometiff_ch)
        .map { row ->
            def (target_json, sample_id, ometiff) = row
            tuple(sample_id, target_json, ometiff)
        }

    import_results = IMPORT_OMETIFF_TO_OMERO(import_input_ch)
    import_reports_ch = import_results.import_reports

    attach_ready_ch = import_reports_ch.join(csv_ch)

    attach_input_ch = target_json_ch
        .combine(attach_ready_ch)
        .map { row ->
            def (target_json, sample_id, import_report, csv) = row
            tuple(sample_id, target_json, import_report, csv)
        }

    csv_ch.view { "CSV_CH => ${it}" }
    import_reports_ch.view { "IMPORT_REPORTS_CH => ${it}" }
    attach_ready_ch.view { "ATTACH_READY_CH => ${it}" }
    attach_input_ch.view { "ATTACH_INPUT_CH => ${it}" }

    attach_results = ATTACH_CSV_TO_OMERO(attach_input_ch, attach_csv_to_image_script_ch)
    attach_reports_ch = attach_results.attach_reports
}

process RUN_WSINSIGHT {

    label 'wsinsight'
    tag "wsinsight"

    secret 'SAML_USERNAME'
    secret 'SAML_PASSWORD'

    input:
    val x

    script:
    def modelArg = params.model_path ? "--model-path ${params.model_path}" : "--model ${params.model}"
    """
    set -euxo pipefail

    export SAML_USERNAME='${params.saml_username}'
    export SAML_PASSWORD='${params.saml_password}'
    export SAML_OTP_CMD='${params.saml_otp_cmd}'

    export SAMLUTIL_BIN='${params.samlutil_bin}'
    export AWS_PROFILE='${params.aws_profile}'
    export AWS_REGION='${params.aws_region}'
    export AWS_DEFAULT_REGION='${params.aws_region}'
    export AWS_DURATION='${params.aws_duration}'

    export HOME="/tmp/wsi_home"
    mkdir -p "\$HOME/.aws"
    chmod 700 "\$HOME" "\$HOME/.aws" || true

    export AWS_SDK_LOAD_CONFIG=1
    export AWS_SHARED_CREDENTIALS_FILE="\$HOME/.aws/credentials"
    export AWS_CONFIG_FILE="\$HOME/.aws/config"

    if ! grep -Fq "[profile \${AWS_PROFILE}]" "\$AWS_CONFIG_FILE" 2>/dev/null; then
        printf '[profile %s]\\nregion=%s\\noutput=json\\n' "\$AWS_PROFILE" "\$AWS_REGION" > "\$AWS_CONFIG_FILE"
    fi

    echo "HOME=\$HOME"
    echo "AWS_SHARED_CREDENTIALS_FILE=\$AWS_SHARED_CREDENTIALS_FILE"
    echo "AWS_CONFIG_FILE=\$AWS_CONFIG_FILE"
    ls -ld "\$HOME" "\$HOME/.aws" || true
    ls -l "\$AWS_CONFIG_FILE" || true

    if [ -z "\${SAML_USERNAME:-}" ] || [ -z "\${SAML_PASSWORD:-}" ]; then
        echo "ERROR: SAML_USERNAME / SAML_PASSWORD not set in environment."
        exit 2
    fi

    echo "=== Refreshing AWS token via samlutil ==="

    python3 - <<'PY'
import os, sys, shlex, subprocess, re
import pexpect

samlutil = os.environ.get("SAMLUTIL_BIN", "/usr/local/bin/samlutil")
region = os.environ.get("AWS_REGION", "us-west-1")
duration = os.environ.get("AWS_DURATION", "12h")

user = os.environ["SAML_USERNAME"]
pw = os.environ["SAML_PASSWORD"]
otp_cmd = os.environ.get("SAML_OTP_CMD")

cmd = f"{shlex.quote(samlutil)} -r {shlex.quote(region)} -d {shlex.quote(str(duration))}"

child = pexpect.spawn(cmd, encoding="utf-8", timeout=300)
child.logfile = sys.stdout
child.delaybeforesend = 0.05

def get_otp():
    if not otp_cmd:
        return None
    return subprocess.check_output(otp_cmd, shell=True, text=True).strip()

USERNAME_RE = re.compile(r"(?i)(please *input *your *username|username) *:")
PASSWORD_RE = re.compile(r"(?i)(please *input *your *password|password) *:")
OTP_RE      = re.compile(r"(?i)(otp|token|passcode|verification *code|mfa)")

patterns = [USERNAME_RE, PASSWORD_RE, OTP_RE, pexpect.EOF, pexpect.TIMEOUT]

while True:
    i = child.expect(patterns)
    if i == 0:
        child.sendline(user)
    elif i == 1:
        child.sendline(pw)
    elif i == 2:
        otp = get_otp()
        if otp is None:
            raise RuntimeError("OTP prompt detected but SAML_OTP_CMD was not set.")
        child.sendline(otp)
    elif i == 3:
        break
    elif i == 4:
        print("\\n--- TIMEOUT DEBUG ---", file=sys.stderr)
        print("Before:\\n", child.before, file=sys.stderr)
        print("After:\\n", child.after, file=sys.stderr)
        raise RuntimeError("Timed out waiting for samlutil prompts/output.")

rc = child.exitstatus if child.exitstatus is not None else 0
sys.exit(rc)
PY

    echo "=== Verifying AWS creds ==="
    aws sts get-caller-identity --profile "\$AWS_PROFILE"

    wsinsight run \
        --wsi-dir "${params.wsi_dir}" \
        --results-dir /workspace/wangc315/wsinsight_results \
        --config "${params.config_json}" \
        ${modelArg} \
        --batch-size ${params.batch_size} \
        --num-workers ${params.num_workers} \
        --omecsv

    echo "WSInsight complete"
    """
}

process STAGE_SVS_FOR_OMERO {

    label 'wsinsight'
    tag "stage_svs"

    input:
    val x

    output:
    path "staged_svs/*.svs", emit: staged_svss
    val true, emit: stage_done

    script:
    """
    set -euxo pipefail

    export SAML_USERNAME='${params.saml_username}'
    export SAML_PASSWORD='${params.saml_password}'
    export SAML_OTP_CMD='${params.saml_otp_cmd}'

    export SAMLUTIL_BIN='${params.samlutil_bin}'
    export AWS_PROFILE='${params.aws_profile}'
    export AWS_REGION='${params.aws_region}'
    export AWS_DEFAULT_REGION='${params.aws_region}'
    export AWS_DURATION='${params.aws_duration}'

    export HOME="/tmp/wsi_home"
    mkdir -p "\$HOME/.aws"
    chmod 700 "\$HOME" "\$HOME/.aws" || true

    export AWS_SDK_LOAD_CONFIG=1
    export AWS_SHARED_CREDENTIALS_FILE="\$HOME/.aws/credentials"
    export AWS_CONFIG_FILE="\$HOME/.aws/config"

    if ! grep -Fq "[profile \${AWS_PROFILE}]" "\$AWS_CONFIG_FILE" 2>/dev/null; then
        printf '[profile %s]\\nregion=%s\\noutput=json\\n' "\$AWS_PROFILE" "\$AWS_REGION" > "\$AWS_CONFIG_FILE"
    fi

    echo "HOME=\$HOME"
    echo "AWS_SHARED_CREDENTIALS_FILE=\$AWS_SHARED_CREDENTIALS_FILE"
    echo "AWS_CONFIG_FILE=\$AWS_CONFIG_FILE"
    ls -ld "\$HOME" "\$HOME/.aws" || true
    ls -l "\$AWS_CONFIG_FILE" || true

    if [ -z "\${SAML_USERNAME:-}" ] || [ -z "\${SAML_PASSWORD:-}" ]; then
        echo "ERROR: SAML_USERNAME / SAML_PASSWORD not set in environment."
        exit 2
    fi

    echo "=== Refreshing AWS token via samlutil ==="

    python3 - <<'PY'
import os, sys, shlex, subprocess, re
import pexpect

samlutil = os.environ.get("SAMLUTIL_BIN", "/usr/local/bin/samlutil")
region = os.environ.get("AWS_REGION", "us-west-1")
duration = os.environ.get("AWS_DURATION", "12h")

user = os.environ["SAML_USERNAME"]
pw = os.environ["SAML_PASSWORD"]
otp_cmd = os.environ.get("SAML_OTP_CMD")

cmd = f"{shlex.quote(samlutil)} -r {shlex.quote(region)} -d {shlex.quote(str(duration))}"

child = pexpect.spawn(cmd, encoding="utf-8", timeout=300)
child.logfile = sys.stdout
child.delaybeforesend = 0.05

def get_otp():
    if not otp_cmd:
        return None
    return subprocess.check_output(otp_cmd, shell=True, text=True).strip()

USERNAME_RE = re.compile(r"(?i)(please *input *your *username|username) *:")
PASSWORD_RE = re.compile(r"(?i)(please *input *your *password|password) *:")
OTP_RE      = re.compile(r"(?i)(otp|token|passcode|verification *code|mfa)")

patterns = [USERNAME_RE, PASSWORD_RE, OTP_RE, pexpect.EOF, pexpect.TIMEOUT]

while True:
    i = child.expect(patterns)
    if i == 0:
        child.sendline(user)
    elif i == 1:
        child.sendline(pw)
    elif i == 2:
        otp = get_otp()
        if otp is None:
            raise RuntimeError("OTP prompt detected but SAML_OTP_CMD was not set.")
        child.sendline(otp)
    elif i == 3:
        break
    elif i == 4:
        print("\\n--- TIMEOUT DEBUG ---", file=sys.stderr)
        print("Before:\\n", child.before, file=sys.stderr)
        print("After:\\n", child.after, file=sys.stderr)
        raise RuntimeError("Timed out waiting for samlutil prompts/output.")

rc = child.exitstatus if child.exitstatus is not None else 0
sys.exit(rc)
PY

    echo "=== Verifying AWS creds ==="
    aws sts get-caller-identity --profile "\$AWS_PROFILE"

    echo "=== Staging SVS files from ${params.wsi_dir} ==="
    mkdir -p staged_svs

    aws s3 cp "${params.wsi_dir}" staged_svs \
        --recursive \
        --exclude "*" \
        --include "*.svs" \
        --profile "\$AWS_PROFILE"

    echo "=== staged files ==="
    ls -lah staged_svs
    find staged_svs -maxdepth 2 | sort

    count=\$(find staged_svs -maxdepth 1 -name '*.svs' | wc -l)
    if [ "\$count" -eq 0 ]; then
        echo "ERROR: No .svs files were staged"
        exit 3
    fi
    """
}

process CONVERT_SVS_TO_ZARR {

    label 'convert'
    tag { slide.baseName }
    stageInMode 'copy'

    publishDir "${params.outdir}/zarr", mode: 'copy'

    input:
    path slide

    output:
    tuple val(slide.baseName), path("${slide.baseName}.zarr"), emit: zarr_dir

    script:
    def stem = slide.baseName
    """
    export PATH="/opt/conda/bin:\$PATH"
    set -euxo pipefail
    umask 022

    echo "started" > started.marker
    pwd > pwd.marker
    ls -lah > ls_before.marker

    echo "=== CONVERT_SVS_TO_ZARR ==="
    echo "PWD: \$(pwd)"
    echo "Input slide: ${slide}"
    echo "Stem: ${stem}"

    ls -lah
    ls -lah "${slide}"

    echo "=== checking tool ==="
    command -v ${params.bioformats2raw_cmd}
    ${params.bioformats2raw_cmd} --version || true

    out_zarr="${stem}.zarr"

    echo "=== running bioformats2raw ==="
    ${params.bioformats2raw_cmd} "${slide}" "\$out_zarr"

    sync || true

    echo "=== files after bioformats2raw ==="
    ls -lah
    find . -maxdepth 5 | sort || true

    if [ ! -d "\$out_zarr" ]; then
        echo "ERROR: Expected Zarr directory was not created: \$out_zarr"
        exit 101
    fi

    if [ -z "\$(find "\$out_zarr" -mindepth 1 -print -quit)" ]; then
        echo "ERROR: Zarr directory exists but is empty: \$out_zarr"
        exit 102
    fi

    chmod -R a+rX "\$out_zarr" || true

    echo "=== final Zarr contents ==="
    ls -ld "\$out_zarr" || true
    find "\$out_zarr" -maxdepth 3 | sort || true
    """
}

process CONVERT_ZARR_TO_OMETIFF {

    label 'convert'
    tag { sample_id }
    stageInMode 'copy'

    publishDir "${params.outdir}/ometiff", mode: 'copy'

    input:
    tuple val(sample_id), path(zarr_dir)

    output:
    tuple val(sample_id), path("${sample_id}.ome.tiff"), emit: ome_tiff

    script:
    """
    set -euxo pipefail
    umask 022
    export PATH="/opt/conda/bin:\$PATH"
    export LD_LIBRARY_PATH="/opt/conda/lib:\$LD_LIBRARY_PATH"

    echo "=== CONVERT_ZARR_TO_OMETIFF ==="
    echo "PWD: \$(pwd)"
    echo "Sample ID: ${sample_id}"
    echo "Zarr input: ${zarr_dir}"

    ls -lah
    ls -ld "${zarr_dir}" || true
    find "${zarr_dir}" -maxdepth 4 | sort || true

    out_tiff="${sample_id}.ome.tiff"

    echo "=== running raw2ometiff ==="
    ${params.raw2ometiff_cmd} "${zarr_dir}" "\$out_tiff" > raw2ometiff.stdout 2> raw2ometiff.stderr

    echo "=== raw2ometiff stdout ==="
    cat raw2ometiff.stdout || true

    echo "=== raw2ometiff stderr ==="
    cat raw2ometiff.stderr || true

    echo "=== files after raw2ometiff ==="
    ls -lah
    find . -maxdepth 5 | sort || true

    if [ ! -f "\$out_tiff" ]; then
        echo "ERROR: Expected OME-TIFF was not created: \$out_tiff"
        exit 201
    fi

    chmod a+r "\$out_tiff" || true
    """
}

process PREPARE_OMERO_TARGET {

    label 'omero'
    tag "omero_target"

    secret 'OMERO_USERNAME'
    secret 'OMERO_PASSWORD'

    input:
    val x
    path project_dataset_script

    output:
    path "omero_target.json", emit: target_json

    script:
    """
    set -euxo pipefail
    export PATH="/opt/conda/bin:\$PATH"

    export HOME="/tmp/omero-home"
    mkdir -p "\$HOME"
    chmod 777 "\$HOME" || true

    export OMERO_USERDIR="\$HOME/.omero"
    export OMERO_SESSIONDIR="\$OMERO_USERDIR/sessions"
    mkdir -p "\$OMERO_USERDIR/java" "\$OMERO_SESSIONDIR"
    chmod 700 "\$OMERO_USERDIR" "\$OMERO_USERDIR/java" "\$OMERO_SESSIONDIR" || true

    cp -Rn /opt/omero-java-cache/* "\$OMERO_USERDIR/java/" || true
    mkdir -p /home/mambauser/.omero/java || true
    cp -Rn /opt/omero-java-cache/* /home/mambauser/.omero/java/ || true

    export OMERO_HOST='${params.omero_host}'
    export OMERO_PORT='${params.omero_port}'
    export OMERO_GROUP='${params.omero_group}'
    export OMERO_PROJECT_NAME='${params.omero_project_name}'
    export OMERO_DATASET_NAME='${params.omero_dataset_name}'
    export OMERO_TARGET_JSON='omero_target.json'

    "${params.omero_python}" "${project_dataset_script}"

    cat omero_target.json
    """
}

process IMPORT_OMETIFF_TO_OMERO {

    label 'omero'
    tag { sample_id }

    secret 'OMERO_USERNAME'
    secret 'OMERO_PASSWORD'

    input:
    tuple val(sample_id), path(target_json), path(ometiff)

    output:
    tuple val(sample_id), path("import_${sample_id}.json"), emit: import_reports
    path "import_*.log", emit: import_logs

    script:
    """
    set -euxo pipefail
    export PATH="/opt/conda/bin:\$PATH"

    export HOME="/tmp/omero-home"
    mkdir -p "\$HOME"
    chmod 777 "\$HOME" || true

    export OMERO_USERDIR="\$HOME/.omero"
    export OMERO_SESSIONDIR="\$OMERO_USERDIR/sessions"
    mkdir -p "\$OMERO_USERDIR/java" "\$OMERO_SESSIONDIR"
    chmod 700 "\$OMERO_USERDIR" "\$OMERO_USERDIR/java" "\$OMERO_SESSIONDIR" || true

    cp -Rn /opt/omero-java-cache/* "\$OMERO_USERDIR/java/" || true
    mkdir -p /home/mambauser/.omero/java || true
    cp -Rn /opt/omero-java-cache/* /home/mambauser/.omero/java/ || true

    export OMERO_HOST='${params.omero_host}'
    export OMERO_PORT='${params.omero_port}'
    export OMERO_GROUP='${params.omero_group}'

    DATASET_ID=\$(${params.omero_python} - <<'PY'
import json
with open("${target_json}", "r", encoding="utf-8") as f:
    data = json.load(f)
print(data["dataset_id"])
PY
)

    LOG_FILE="import_${sample_id}.log"
    REPORT_FILE="import_${sample_id}.json"
    RETRY_COUNT='${params.omero_retry_count}'
    RETRY_SLEEP='${params.omero_retry_sleep}'
    LOGIN_TIMEOUT='${params.omero_login_timeout}'

    login_omero() {
        set +x
        if [ -n "\$OMERO_GROUP" ]; then
            "${params.omero_cli}" login --timeout "\$LOGIN_TIMEOUT" -g "\$OMERO_GROUP" "\${OMERO_USERNAME}@\${OMERO_HOST}:\${OMERO_PORT}" -w "\$OMERO_PASSWORD"
        else
            "${params.omero_cli}" login --timeout "\$LOGIN_TIMEOUT" "\${OMERO_USERNAME}@\${OMERO_HOST}:\${OMERO_PORT}" -w "\$OMERO_PASSWORD"
        fi
        set -x
    }

    # Use OMERO_PASSWORD env var instead of -w to avoid shell mangling/log leakage
    export OMERO_PASSWORD="\$OMERO_PASSWORD"

    # Login with retries
    login_ok=0
    for attempt in \$(seq 1 "\$RETRY_COUNT"); do
      if login_omero; then
        login_ok=1
        break
      fi
      echo "OMERO login failed on attempt \$attempt/\$RETRY_COUNT; sleeping \$RETRY_SLEEP sec"
      sleep "\$RETRY_SLEEP"
    done
    if [ "\$login_ok" -ne 1 ]; then
      echo "ERROR: OMERO login failed after \$RETRY_COUNT attempts"
      exit 90
    fi

    # Import with retries
    import_ok=0
    for attempt in \$(seq 1 "\$RETRY_COUNT"); do
      set +e
      "${params.omero_cli}" import -d "\$DATASET_ID" "${ometiff}" > "\$LOG_FILE" 2>&1
      rc=\$?
      set -e
      cat "\$LOG_FILE" || true
      if [ "\$rc" -eq 0 ]; then
        import_ok=1
        break
      fi
      echo "OMERO import failed on attempt \$attempt/\$RETRY_COUNT with rc=\$rc; sleeping \$RETRY_SLEEP sec"
      sleep "\$RETRY_SLEEP"
      # Refresh session before retry
      set +e
      "${params.omero_cli}" logout >/dev/null 2>&1
      set -e
      login_omero
    done
    if [ "\$import_ok" -ne 1 ]; then
      echo "ERROR: OMERO import failed after \$RETRY_COUNT attempts"
      exit 91
    fi

    ${params.omero_python} - <<PY > "\$REPORT_FILE"
import json, re, pathlib

sample_id = "${sample_id}"
ometiff = "${ometiff}"
dataset_id = int("${'$'}DATASET_ID")
log_file = "${'$'}LOG_FILE"

text = pathlib.Path(log_file).read_text(encoding="utf-8", errors="replace")

image_ids = []
m = re.search(r"Image:([0-9,]+)", text)
if m:
    image_ids = [int(x) for x in m.group(1).split(",") if x.strip()]

report = {
    "sample_id": sample_id,
    "dataset_id": dataset_id,
    "ometiff": ometiff,
    "log_file": log_file,
    "image_ids": image_ids,
    "status": "imported"
}
print(json.dumps(report, indent=2))
PY

    cat "\$REPORT_FILE"
    "${params.omero_cli}" logout || true
    """
}

process ATTACH_CSV_TO_OMERO {

    label 'omero'
    tag { sample_id }

    secret 'OMERO_USERNAME'
    secret 'OMERO_PASSWORD'

    input:
    tuple val(sample_id), path(target_json), path(import_report), path(csv)
    path attach_csv_to_image_script

    output:
    tuple val(sample_id), path("attach_${sample_id}.json"), emit: attach_reports

    script:
    """
    set -euxo pipefail
    export PATH="/opt/conda/bin:\$PATH"

    export HOME="/tmp/omero-home"
    mkdir -p "\$HOME"
    chmod 777 "\$HOME" || true

    export OMERO_USERDIR="\$HOME/.omero"
    export OMERO_SESSIONDIR="\$OMERO_USERDIR/sessions"
    mkdir -p "\$OMERO_USERDIR/java" "\$OMERO_SESSIONDIR"
    chmod 700 "\$OMERO_USERDIR" "\$OMERO_USERDIR/java" "\$OMERO_SESSIONDIR" || true

    cp -Rn /opt/omero-java-cache/* "\$OMERO_USERDIR/java/" || true
    mkdir -p /home/mambauser/.omero/java || true
    cp -Rn /opt/omero-java-cache/* /home/mambauser/.omero/java/ || true

    export OMERO_HOST='${params.omero_host}'
    export OMERO_PORT='${params.omero_port}'
    export OMERO_GROUP='${params.omero_group}'

    export IMPORT_REPORT_JSON='${import_report}'
    export CSV_PATH='${csv}'
    export SAMPLE_ID='${sample_id}'

    OUT="attach_${sample_id}.json"
    RETRY_COUNT='${params.omero_retry_count}'
    RETRY_SLEEP='${params.omero_retry_sleep}'
    LOGIN_TIMEOUT='${params.omero_login_timeout}'

    login_omero() {
      set +x
      if [ -n "\$OMERO_GROUP" ]; then
        "${params.omero_cli}" login --timeout "\$LOGIN_TIMEOUT" -g "\$OMERO_GROUP" "\${OMERO_USERNAME}@\${OMERO_HOST}:\${OMERO_PORT}" -w "\$OMERO_PASSWORD"
      else
        "${params.omero_cli}" login --timeout "\$LOGIN_TIMEOUT" "\${OMERO_USERNAME}@\${OMERO_HOST}:\${OMERO_PORT}" -w "\$OMERO_PASSWORD"
      fi
      set -x
    }

    login_omero

    "${params.omero_python}" "${attach_csv_to_image_script}" > "\$OUT"

    cat "\$OUT"

    "${params.omero_cli}" logout || true
    """
}
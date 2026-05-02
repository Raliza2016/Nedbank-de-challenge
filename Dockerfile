FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# Install extra runtime dependencies (none beyond base image — kept for forward compat)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration.
# tests/ is intentionally NOT copied — the suite is a developer-time tool,
# not a runtime artifact, and pytest is not on the base image.
COPY pipeline/ ./pipeline/
COPY config/   ./config/

# Make the pipeline package importable from /app
ENV PYTHONPATH=/app
# NOTE: PIPELINE_CONFIG / DQ_RULES_PATH are intentionally NOT set here.
#
# The scoring harness contract guarantees /data/config/pipeline_config.yaml
# is mounted, but does NOT guarantee dq_rules.yaml. Hard-coding the env vars
# would force the resolver into its fail-loud branch (FileNotFoundError) if
# dq_rules.yaml is absent. By leaving them unset, the resolver:
#   1. checks /data/config/ first  (operator-supplied via the data mount)
#   2. falls back to /app/config/   (baked into this image — always present)
# This makes the image robust whether the harness provides 0, 1, or 2 configs.
ENV PIPELINE_LOG_LEVEL=INFO

# Unbuffered stdout so log lines appear in real time during scoring.
ENV PYTHONUNBUFFERED=1

# The scoring system invokes: docker run ... python pipeline/run_all.py
CMD ["python", "pipeline/run_all.py"]

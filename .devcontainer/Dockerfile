ARG GO_VERSION=1.25
ARG GO_IMAGE=mcr.microsoft.com/devcontainers/go
ARG ALPINE_VERSION=bookworm

FROM ${GO_IMAGE}:${GO_VERSION}-${ALPINE_VERSION}

# Install dependencies
RUN apt update \
    && apt install -yqq jq tmux \
    && apt clean -yqq  || true

# Update non-root user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Switch to non-root user
USER $USERNAME
WORKDIR /workspace

# Add tmux config (or copy from repo)
COPY tmux.conf /root/$USERNAME/.tmux.conf

# Install protoc Go plugins
RUN go install github.com/go-task/task/v3/cmd/task@latest || true && \
    go install github.com/Zxilly/go-size-analyzer/cmd/gsa@latest || true
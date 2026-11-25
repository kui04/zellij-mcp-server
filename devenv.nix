{
    pkgs,
    config,
    inputs,
    ...
}:
let
    rustOverlayPkgs = pkgs.extend inputs.rust-overlay.overlays.default;

    # rustfmt.toml enables unstable options (imports_granularity, group_imports),
    # which stable rustfmt rejects. Only rustfmt comes from nightly; rustc, cargo
    # and clippy stay on the locked stable toolchain.
    nightlyRustfmt = rustOverlayPkgs.rust-bin.nightly.latest.rustfmt;
    rustToolchain = config.languages.rust.toolchainPackage;

    # nixpkgs still ships zellij 0.46.0, which has no `zellij events` command.
    # The MCP event bus needs 0.46.1 or newer. Use the official static build for
    # x86_64-linux until nixpkgs catches up.
    zellij = pkgs.stdenvNoCC.mkDerivation (finalAttrs: {
        pname = "zellij";
        version = "0.46.1";

        src = pkgs.fetchurl {
            url = "https://github.com/zellij-org/zellij/releases/download/v${finalAttrs.version}/zellij-x86_64-unknown-linux-musl.tar.gz";
            hash = "sha256-FjMhE1fCNgU7UdGuZBXCP3/3Gqro4IcxXkmc7KGxf6Q=";
        };

        sourceRoot = ".";
        dontConfigure = true;
        dontBuild = true;
        dontFixup = true;

        installPhase = ''
            install -Dm755 zellij $out/bin/zellij
        '';

        meta = {
            description = "Terminal workspace with panes, tabs and layouts";
            homepage = "https://zellij.dev";
            license = pkgs.lib.licenses.mit;
            mainProgram = "zellij";
            platforms = [ "x86_64-linux" ];
            sourceProvenance = [ pkgs.lib.sourceTypes.binaryNativeCode ];
        };
    });
in
{
    languages.rust = {
        enable = true;
        channel = "stable";
        version = "latest";
        components = [
            "rustc"
            "cargo"
            "clippy"
            "rustfmt"
            "rust-analyzer"
        ];
        toolchain.rustfmt = nightlyRustfmt;
    };

    # cargo-fmt resolves RUSTFMT before the rustfmt beside cargo, so plain
    # `cargo fmt` formats with the nightly binary too.
    env.RUSTFMT = "${nightlyRustfmt}/bin/rustfmt";

    packages = [
        pkgs.git
        pkgs.nixfmt
        # The server drives a real zellij process, so integration tests need it.
        zellij
    ];

    scripts = {
        # `fmt` is already a coreutils program, so the formatting alias needs a
        # different name to be reachable on PATH.
        format.exec = ''
            cargo fmt --all "$@"
        '';
        fmt-check.exec = ''
            cargo fmt --all -- --check
        '';
        lint.exec = ''
            cargo clippy --all-targets -- -D warnings
        '';
    };

    git-hooks = {
        enable = true;
        hooks = {
            rustfmt = {
                enable = true;
                packageOverrides = {
                    cargo = rustToolchain;
                    rustfmt = nightlyRustfmt;
                };
            };

            clippy = {
                enable = true;
                packageOverrides = {
                    cargo = rustToolchain;
                    clippy = rustToolchain;
                };
                settings = {
                    allFeatures = true;
                    denyWarnings = true;
                    extraArgs = "--all-targets";
                };
            };

            nixfmt = {
                enable = true;
                settings.indent = 4;
            };
            deadnix.enable = true;
            statix.enable = true;

            prettier = {
                enable = true;
                files = "\\.md$";
                settings = {
                    print-width = 100;
                    prose-wrap = "always";
                };
            };
        };
    };

    enterShell = ''
        echo "zellij-mcp-server dev shell:"
        echo "  rustc:   $(rustc --version)"
        echo "  rustfmt: $(rustfmt --version)"
        echo "  zellij:  $(zellij --version 2>/dev/null || echo 'not available')"
    '';

    enterTest = ''
        set -euo pipefail
        cargo fmt --all -- --check
        cargo clippy --all-targets -- -D warnings
        cargo test
    '';
}

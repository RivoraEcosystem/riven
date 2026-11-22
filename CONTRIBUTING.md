# Contributing to Riven

Riven is under active development, and contributions are welcome. The project is evolving, so discuss larger changes before investing time in an implementation.

## Before you start

- For a bug in Riven, search existing issues first. If it has not been reported, open an [issue](https://github.com/RivoraEcosystem/riven/issues) with steps to reproduce and relevant details.
- For a feature idea, question, or design proposal, search existing [discussions](https://github.com/RivoraEcosystem/riven/discussions) and start a discussion if needed. The maintainers will decide whether and when a proposed feature fits the project.
- Check open issues and pull requests before starting work. If an issue or implementation is already being handled, coordinate in that existing thread instead of opening a duplicate issue or pull request. You can ask questions or offer help by commenting there.
- Spamming issues or pull requests, or creating duplicate issues or pull requests, will result in a ban from the project.
- Bugs in `RCP` or `aioquic` should be reported to their respective projects: [RCP issues](https://github.com/RivoraEcosystem/rcp/issues) and [aioquic issues](https://github.com/aiortc/aioquic/issues).
- To get context on an issue or the project direction, comment on its issue or discussion, or contact the maintainer using the details in the [README](README.md#contributions).

## Reporting protocol compliance bugs

When reporting a suspected violation of HTTP/3, WebTransport, or RCP requirements, include a link to the relevant original specification or official project documentation and identify the section that establishes the requirement. Explain how Riven's behavior differs from that requirement.

Riven's WebTransport implementation is under development against [WebTransport over HTTP/3 draft-16](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-16). A behavior that differs from another draft version is not an active bug if it conforms to draft-16. Check the applicable draft-16 requirement before reporting a version difference as a bug.

## Areas to contribute

Useful areas for code contributions include performance and efficiency improvements, and WebTransport development.

Dedicated documentation for Riven is also welcome. Please explore Riven's code and public API, then document how to use Riven, its features, and how those features work.

## Set up a development environment

Riven requires Python 3.13 or newer. Clone the repository:

```bash
git clone https://github.com/RivoraEcosystem/riven.git
cd riven
```

Upgrade pip to a version that supports dependency groups, then install Riven in editable mode with the `dev` group:

```bash
python -m pip install --upgrade pip
python -m pip install -e . --group dev
```

The `dev` group installs the lint, spelling, and manifest-checking tools used by CI. Packaging tools are not part of this group.

## Pull requests

- Keep each pull request focused and describe the problem and the change.
- Link the related issue or discussion when one exists. If work is already in progress, coordinate in its existing thread before preparing a pull request.
- Include relevant verification details in the pull request description.


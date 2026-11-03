"""Container readiness probe for the control service."""

from urllib.request import urlopen

from control_service.config import ControlSettings


def main() -> None:
    settings = ControlSettings()
    with urlopen(
        f"http://127.0.0.1:{settings.api_port}/health/ready",
        timeout=2,
    ) as response:
        if response.status != 200:
            raise RuntimeError(f"readiness probe returned HTTP {response.status}")


if __name__ == "__main__":
    main()

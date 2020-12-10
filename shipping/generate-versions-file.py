import json
import re

with open("src/panoramic/cli/__version__.py") as f:
    data = f.read()
    version = re.search(r'__version__ = "(.*?)"', data).group(1)  # type: ignore
    minimum_supported_version = re.search(r'__minimum_supported_version__ = "(.*?)"', data).group(1)  # type: ignore

with open("versions.json", "w") as f:
    data = json.dumps(
        {
            "minimum_supported_version": minimum_supported_version,
            "latest_version": version,
            "analytics_write_key": "1tQRCoY7ZsrBvKMXdOxiVtQMQNAOQ6RO",
        }
    )
    f.write(data)

print(version)

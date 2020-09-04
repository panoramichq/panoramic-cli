import yaml

from panoramic.cli.context import get_company_slug
from panoramic.cli.paths import Paths


def test_context_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with open(Paths.context_file(), 'w') as f:
        f.write(yaml.dump(dict(api_version='v2', company_slug='company_name_12fxs')))

    # Use __wrapped__ to avoid cache
    assert get_company_slug.__wrapped__() == 'company_name_12fxs'

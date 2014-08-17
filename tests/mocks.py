import json
import mock


class MockRedis(mock.Mock):
    pull_data = None
    pull_keys = []
    pull_dict = {}
    longbox_data = None
    seen_keys = []
    seen_dict = []

    @classmethod
    def _load_pull_data(cls, test_data_filename):
        with open(test_data_filename) as json_file:
            cls.pull_data = json.load(json_file)
        cls.pull_keys = ["pull:%s" % p["identifier"] for p in cls.pull_data]
        cls.pull_dict = {
            key: json.dumps(value) for key, value in zip(
                cls.pull_keys, cls.pull_data)
        }

    @classmethod
    def _load_longbox_data(cls, test_data_filename):
        with open(test_data_filename) as json_file:
            cls.longbox_data = json.load(json_file)
        cls.seen_keys = cls.longbox_data.keys()
        cls.seen_dict = {
            "gs:seen:%s" % key: json.dumps(value) for key, value in
            cls.longbox_data.items()
        }

    fetch_unread = mock.Mock(side_effect=lambda: MockRedis.pull_data)
    get = mock.Mock(side_effect=lambda k: MockRedis.pull_dict.get(k))
    keys = mock.Mock(side_effect=lambda k: MockRedis.pull_keys)
    sismember = mock.Mock(side_effect=lambda s, k: k in MockRedis.seen_keys)

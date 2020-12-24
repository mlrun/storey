from .drivers import Driver
from .utils import _split_path


class Table:
    """
        Table object, represents a single table in a specific storage.

        :param table_path: Path to the table in the storage.
        :param storage: Storage driver
        :param partitioned_by_key: Whether that data is partitioned by the key or not, based on this indication storage drivers
         can optimize writes. Defaults to True.
        """

    def __init__(self, table_path: str, storage: Driver, partitioned_by_key: bool = True):
        self._container, self._table_path = _split_path(table_path)
        self._storage = storage
        self._cache = {}
        self._aggregation_store = None
        self._partitioned_by_key = partitioned_by_key

    def __getitem__(self, key):
        return self._cache[key]

    def __setitem__(self, key, value):
        self._cache[key] = value

    def update_key(self, key, data):
        if key in self._cache:
            for name, value in data.items():
                self._cache[key][name] = value
        else:
            self._cache[key] = data

    async def lazy_load_key_with_aggregates(self, key, timestamp=None):
        if self._aggregation_store._read_only or key not in self._aggregation_store:
            # Try load from the store, and create a new one only if the key really is new
            aggregate_initial_data, additional_data = await self._storage._load_aggregates_by_key(self._container, self._table_path, key)

            # Create new aggregation element
            await self._aggregation_store.add_key(key, timestamp, aggregate_initial_data)

            if additional_data:
                # Add additional data to simple cache
                self.update_key(key, additional_data)

    async def get_or_load_key(self, key, attributes='*'):
        if key not in self._cache:
            res = await self._storage._load_by_key(self._container, self._table_path, key, attributes)
            if res:
                self._cache[key] = res
            else:
                self._cache[key] = {}
        return self._cache[key]

    def _set_aggregation_store(self, store):
        store._container = self._container
        store._table_path = self._table_path
        store._storage = self._storage
        self._aggregation_store = store

    async def persist_key(self, key, event_data_to_persist):
        aggr_by_key = None
        if self._aggregation_store:
            aggr_by_key = self._aggregation_store[key]
        additional_data_persist = self._cache.get(key, None)
        if event_data_to_persist:
            if not additional_data_persist:
                additional_data_persist = event_data_to_persist
            else:
                additional_data_persist.update(event_data_to_persist)
        await self._storage._save_key(self._container, self._table_path, key, aggr_by_key, self._partitioned_by_key,
                                      additional_data_persist)

    async def close(self):
        await self._storage.close()

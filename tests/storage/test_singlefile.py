from __future__ import annotations

import pytest

from vdirsyncer.storage.singlefile import SingleFileStorage

from . import StorageTests


class TestSingleFileStorage(StorageTests):
    storage_class = SingleFileStorage
    supports_metadata = False

    @pytest.fixture
    def get_storage_args(self, tmpdir):
        async def inner(collection="test"):
            rv = {"path": str(tmpdir.join("%s.txt")), "collection": collection}
            if collection is not None:
                rv = await self.storage_class.create_collection(**rv)
            return rv

        return inner

    @pytest.mark.asyncio
    async def test_delete_empty_collection(self, tmpdir):
        args = await self.storage_class.create_collection(
            collection="c", path=str(tmpdir.join("%s.txt"))
        )
        storage = self.storage_class(**args)
        await storage.delete_collection()
        assert not tmpdir.join("c.txt").check()

    @pytest.mark.asyncio
    async def test_delete_collection_is_idempotent_when_missing(self, tmpdir):
        args = await self.storage_class.create_collection(
            collection="c", path=str(tmpdir.join("%s.txt"))
        )
        storage = self.storage_class(**args)
        tmpdir.join("c.txt").remove()
        # Must not raise even though the file is already gone.
        await storage.delete_collection()

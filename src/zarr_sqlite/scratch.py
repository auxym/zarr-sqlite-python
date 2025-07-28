
class SQLiteStore:
    supports_writes: bool = True
    supports_deletes: bool = True
    supports_partial_writes: bool = True
    supports_listing: bool = True

    root: str

    def __init__(self, root: str) -> None:
        self.root = root

a = SQLiteStore("aaa")
b = SQLiteStore("bbb")

print(a.root)
print(b.root)

a.root = "ccc"
print(a.root)
print(b.root)

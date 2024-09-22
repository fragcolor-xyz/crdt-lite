import unittest
import uuid
from collections import defaultdict


class LogicalClock:
    def __init__(self):
        self.time = 0

    def tick(self):
        """Increment the clock for a local event."""
        self.time += 1
        return self.time

    def update(self, received_time):
        """Update the clock when receiving a timestamp from another node."""
        self.time = max(self.time, received_time)
        self.time += 1
        return self.time

    def __repr__(self):
        return f"LogicalClock({self.time})"


class FormTags:
    def __init__(self, node_id):
        self.node_id = node_id  # Unique identifier for this node
        self.clock = LogicalClock()
        self.data = {}  # Keyed by id
        self.crsql_clock = defaultdict(dict)  # key -> col_name -> clock info
        self.tombstones = set()  # Set of deleted ids

    def insert(self, record):
        record_id = record["id"]

        # Prevent re-insertion if the record is already tombstoned
        if record_id in self.tombstones:
            print(
                f"Insert ignored: Record {record_id} is already deleted (tombstoned)."
            )
            return

        db_version = self.clock.tick()

        # Insert record
        self.data[record_id] = record.copy()

        # Initialize clock entries for each column
        for col_name in record:
            self.crsql_clock[record_id][col_name] = {
                "col_version": 1,  # Initial version
                "db_version": db_version,
                "site_id": self.node_id,
                "seq": 0,  # For ordering within the same db_version
            }

    def update(self, record_id, updates):
        if record_id in self.tombstones:
            print(f"Update ignored: Record {record_id} is deleted (tombstoned).")
            return

        if record_id in self.data:
            db_version = self.clock.tick()

            for col_name, value in updates.items():
                # Update the value
                self.data[record_id][col_name] = value

                # Update the clock for this column
                col_info = self.crsql_clock[record_id].get(
                    col_name,
                    {
                        "col_version": 0,
                        "db_version": 0,
                        "site_id": self.node_id,
                        "seq": 0,
                    },
                )
                col_version = col_info["col_version"] + 1
                self.crsql_clock[record_id][col_name] = {
                    "col_version": col_version,
                    "db_version": db_version,
                    "site_id": self.node_id,
                    "seq": col_info.get("seq", 0) + 1,  # Increment seq
                }
        else:
            print(f"Update ignored: Record with id {record_id} does not exist.")

    def delete(self, record_id):
        if record_id in self.tombstones:
            print(
                f"Delete ignored: Record {record_id} is already deleted (tombstoned)."
            )
            return

        db_version = self.clock.tick()
        # Mark as tombstone
        self.tombstones.add(record_id)
        # Remove data and clock info
        self.data.pop(record_id, None)
        self.crsql_clock.pop(record_id, None)
        # Update tombstone clock
        self.crsql_clock[record_id]["__deleted__"] = {
            "col_version": 1,
            "db_version": db_version,
            "site_id": self.node_id,
            "seq": 0,
        }

    def get_changes_since(self, last_db_version):
        changes = []
        # Include data changes
        for record_id, columns in self.crsql_clock.items():
            for col_name, clock_info in columns.items():
                if clock_info["db_version"] > last_db_version:
                    value = None
                    if col_name != "__deleted__":
                        value = self.data.get(record_id, {}).get(col_name)
                    change = {
                        "id": record_id,
                        "col_name": col_name,
                        "value": value,
                        "col_version": clock_info["col_version"],
                        "db_version": clock_info["db_version"],
                        "site_id": clock_info["site_id"],
                        "seq": clock_info["seq"],
                    }
                    changes.append(change)
        return changes

    def merge_changes(self, changes):
        for change in changes:
            record_id = change["id"]
            col_name = change["col_name"]
            remote_col_version = change["col_version"]
            remote_db_version = change["db_version"]
            remote_site_id = change["site_id"]
            remote_seq = change["seq"]
            remote_value = change["value"]

            # Update logical clock
            self.clock.update(remote_db_version)

            local_col_info = self.crsql_clock.get(record_id, {}).get(col_name)

            # Determine if we should accept the remote change
            should_accept = False

            if not local_col_info:
                should_accept = True
            else:
                local_col_version = local_col_info["col_version"]
                if remote_col_version > local_col_version:
                    should_accept = True
                elif remote_col_version == local_col_version:
                    # Prioritize deletions over inserts/updates
                    if (
                        col_name == "__deleted__"
                        and change["col_name"] != "__deleted__"
                    ):
                        should_accept = True
                    elif (
                        change["col_name"] == "__deleted__"
                        and col_name != "__deleted__"
                    ):
                        should_accept = False
                    elif (
                        change["col_name"] == "__deleted__"
                        and col_name == "__deleted__"
                    ):
                        # If both are deletions, use site_id and seq as tie-breakers
                        if remote_site_id > local_col_info["site_id"]:
                            should_accept = True
                        elif remote_site_id == local_col_info["site_id"]:
                            if remote_seq > local_col_info["seq"]:
                                should_accept = True
                    else:
                        # Tie-breaker using site ID and seq
                        if remote_site_id > local_col_info["site_id"]:
                            should_accept = True
                        elif remote_site_id == local_col_info["site_id"]:
                            if remote_seq > local_col_info["seq"]:
                                should_accept = True

            if should_accept:
                if col_name == "__deleted__":
                    # Handle deletion
                    self.tombstones.add(record_id)
                    self.data.pop(record_id, None)
                    self.crsql_clock[record_id] = {
                        "__deleted__": {
                            "col_version": remote_col_version,
                            "db_version": remote_db_version,
                            "site_id": remote_site_id,
                            "seq": remote_seq,
                        }
                    }
                else:
                    # Accept the value
                    if record_id not in self.data:
                        self.data[record_id] = {}
                    self.data[record_id][col_name] = remote_value

                    # Update clock info
                    if record_id not in self.crsql_clock:
                        self.crsql_clock[record_id] = {}
                    self.crsql_clock[record_id][col_name] = {
                        "col_version": remote_col_version,
                        "db_version": remote_db_version,
                        "site_id": remote_site_id,
                        "seq": remote_seq,
                    }

    def print_data(self):
        print(f"Node {self.node_id} Data:")
        for record_id, record in self.data.items():
            print(f"ID: {record_id}")
            for key, value in record.items():
                print(f"  {key}: {value}")
        print(f"Tombstones: {self.tombstones}")
        print()

    def get_db_version(self):
        return self.clock.time


# Unit Tests
class TestCRDT(unittest.TestCase):
    def setUp(self):
        # Initialize two nodes
        self.node1 = FormTags(node_id=1)
        self.node2 = FormTags(node_id=2)

    def test_basic_insert_and_merge(self):
        # Node1 inserts a record
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "form_id": str(uuid.uuid4()),
            "tag": "Node1Tag",
            "created_at": "2023-10-01T12:00:00Z",
            "created_by": "User1",
        }
        self.node1.insert(record)

        # Node2 inserts the same record with different data
        record2 = record.copy()
        record2["tag"] = "Node2Tag"
        record2["created_by"] = "User2"
        self.node2.insert(record2)

        # Merge node2 into node1
        changes_from_node2 = self.node2.get_changes_since(0)
        self.node1.merge_changes(changes_from_node2)

        # Merge node1 into node2
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)

        # Both nodes should resolve the conflict and have the same data
        self.assertEqual(self.node1.data, self.node2.data)
        self.assertEqual(self.node1.data[record_id]["tag"], "Node2Tag")
        self.assertEqual(self.node1.data[record_id]["created_by"], "User2")

    def test_updates_with_conflicts(self):
        # Insert a shared record
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "tag": "InitialTag",
        }
        self.node1.insert(record)
        self.node2.insert(record)

        # Node1 updates 'tag'
        self.node1.update(record_id, {"tag": "Node1UpdatedTag"})

        # Node2 updates 'tag'
        self.node2.update(record_id, {"tag": "Node2UpdatedTag"})

        # Merge changes
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)
        changes_from_node2 = self.node2.get_changes_since(0)
        self.node1.merge_changes(changes_from_node2)

        # Conflict resolved
        # Since col_versions are equal, tie-breaker is site_id
        expected_tag = (
            "Node2UpdatedTag"
            if self.node2.node_id > self.node1.node_id
            else "Node1UpdatedTag"
        )
        self.assertEqual(self.node1.data[record_id]["tag"], expected_tag)
        self.assertEqual(self.node1.data, self.node2.data)

    def test_delete_and_merge(self):
        # Insert and sync a record
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "tag": "ToBeDeleted",
        }
        self.node1.insert(record)
        changes = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes)

        # Node1 deletes the record
        self.node1.delete(record_id)

        # Merge the deletion to node2
        changes = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes)

        # Both nodes should reflect the deletion
        self.assertNotIn(record_id, self.node1.data)
        self.assertNotIn(record_id, self.node2.data)
        self.assertIn(record_id, self.node1.tombstones)
        self.assertIn(record_id, self.node2.tombstones)

    def test_tombstone_handling(self):
        # Insert a record and delete it on node1
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "tag": "Temporary",
        }
        self.node1.insert(record)
        self.node1.delete(record_id)

        # Node2 inserts the same record
        self.node2.insert(record)

        # Merge changes
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)

        # Node2 should respect the tombstone
        self.assertNotIn(record_id, self.node2.data)
        self.assertIn(record_id, self.node2.tombstones)

    def test_conflict_resolution_with_site_id_and_seq(self):
        # Both nodes insert a record with the same id
        record_id = str(uuid.uuid4())
        record1 = {
            "id": record_id,
            "tag": "Node1Tag",
        }
        record2 = {
            "id": record_id,
            "tag": "Node2Tag",
        }
        self.node1.insert(record1)
        self.node2.insert(record2)

        # Both nodes update the 'tag' field multiple times
        self.node1.update(record_id, {"tag": "Node1Tag1"})
        self.node1.update(record_id, {"tag": "Node1Tag2"})

        self.node2.update(record_id, {"tag": "Node2Tag1"})
        self.node2.update(record_id, {"tag": "Node2Tag2"})

        # Merge changes
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)
        changes_from_node2 = self.node2.get_changes_since(0)
        self.node1.merge_changes(changes_from_node2)

        # The node with the higher site_id and seq should win
        expected_tag = (
            "Node2Tag2" if self.node2.node_id > self.node1.node_id else "Node1Tag2"
        )
        self.assertEqual(self.node1.data[record_id]["tag"], expected_tag)
        self.assertEqual(self.node1.data, self.node2.data)

    def test_logical_clock_update(self):
        # Node1 inserts a record
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "tag": "Node1Tag",
        }
        self.node1.insert(record)

        # Node2 receives the change
        changes = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes)

        # Node2's clock should be updated
        self.assertTrue(self.node2.clock.time > 0)
        self.assertTrue(self.node2.clock.time >= self.node1.clock.time)

    def test_merge_without_conflicts(self):
        # Node1 inserts a record
        record_id1 = str(uuid.uuid4())
        record1 = {
            "id": record_id1,
            "tag": "Node1Record",
        }
        self.node1.insert(record1)

        # Node2 inserts a different record
        record_id2 = str(uuid.uuid4())
        record2 = {
            "id": record_id2,
            "tag": "Node2Record",
        }
        self.node2.insert(record2)

        # Merge changes
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)
        changes_from_node2 = self.node2.get_changes_since(0)
        self.node1.merge_changes(changes_from_node2)

        # Both nodes should have both records
        self.assertIn(record_id1, self.node1.data)
        self.assertIn(record_id2, self.node1.data)
        self.assertIn(record_id1, self.node2.data)
        self.assertIn(record_id2, self.node2.data)
        self.assertEqual(self.node1.data, self.node2.data)

    def test_multiple_merges(self):
        # Node1 inserts a record
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "tag": "InitialTag",
        }
        self.node1.insert(record)

        # Merge to node2
        changes = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes)

        # Node2 updates the record
        self.node2.update(record_id, {"tag": "UpdatedByNode2"})

        # Node1 updates the record
        self.node1.update(record_id, {"tag": "UpdatedByNode1"})

        # Merge changes
        changes_from_node2 = self.node2.get_changes_since(0)
        self.node1.merge_changes(changes_from_node2)
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)

        # Conflict resolved
        expected_tag = (
            "UpdatedByNode2"
            if self.node2.node_id > self.node1.node_id
            else "UpdatedByNode1"
        )
        self.assertEqual(self.node1.data[record_id]["tag"], expected_tag)
        self.assertEqual(self.node1.data, self.node2.data)

    def test_inserting_after_deletion(self):
        # Node1 inserts and deletes a record
        record_id = str(uuid.uuid4())
        record = {
            "id": record_id,
            "tag": "Temporary",
        }
        self.node1.insert(record)
        self.node1.delete(record_id)

        # Merge deletion to node2
        changes_from_node1 = self.node1.get_changes_since(0)
        self.node2.merge_changes(changes_from_node1)

        # Node2 tries to insert the same record
        self.node2.insert(record)

        # Merge changes
        changes_from_node2 = self.node2.get_changes_since(0)
        self.node1.merge_changes(changes_from_node2)

        # The deletion should prevail
        self.assertNotIn(record_id, self.node1.data)
        self.assertNotIn(record_id, self.node2.data)
        self.assertIn(record_id, self.node1.tombstones)
        self.assertIn(record_id, self.node2.tombstones)


# Run the tests
if __name__ == "__main__":
    unittest.main(argv=[""], exit=False)

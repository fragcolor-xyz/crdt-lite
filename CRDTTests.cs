using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using ForSync.CRDT;

[TestFixture]
public class CRDTTests
{
    // Helper function to generate unique IDs (simulating UUIDs)
    private static string GenerateUuid()
    {
        return Guid.NewGuid().ToString();
    }

    // Helper function to create a dictionary with ColName<string> keys
    private static Dictionary<ColName<string>, string> CreateFieldsDictionary(Dictionary<string, string> fields)
    {
        return fields.ToDictionary(kvp => new ColName<string>(kvp.Key), kvp => kvp.Value);
    }

    [Test]
    public void BasicInsertAndMerge()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Node1 inserts a record
        string recordId = GenerateUuid();
        string formId = GenerateUuid();
        var fields1 = CreateFieldsDictionary(new Dictionary<string, string>
        {
            {"id", recordId},
            {"form_id", formId},
            {"tag", "Node1Tag"},
            {"created_at", "2023-10-01T12:00:00Z"},
            {"created_by", "User1"}
        });
        var changes1 = node1.InsertOrUpdate(recordId, fields1);

        // Node2 inserts the same record with different data
        var fields2 = CreateFieldsDictionary(new Dictionary<string, string>
        {
            {"id", recordId},
            {"form_id", formId},
            {"tag", "Node2Tag"},
            {"created_at", "2023-10-01T12:05:00Z"},
            {"created_by", "User2"}
        });
        var changes2 = node2.InsertOrUpdate(recordId, fields2);

        // Merge node2's changes into node1
        node1.MergeChanges(changes2);

        // Merge node1's changes into node2
        node2.MergeChanges(changes1);

        // Both nodes should resolve the conflict and have the same data
        Assert.That(node1.GetData(), Is.EqualTo(node2.GetData()), "Basic Insert and Merge: Data mismatch");
        Assert.That(node1.GetData()[recordId].Fields[new ColName<string>("tag")], Is.EqualTo("Node2Tag"), "Basic Insert and Merge: Tag should be 'Node2Tag'");
        Assert.That(node1.GetData()[recordId].Fields[new ColName<string>("created_by")], Is.EqualTo("User2"), "Basic Insert and Merge: created_by should be 'User2'");
    }

    [Test]
    public void UpdatesWithConflicts()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Insert a shared record
        string recordId = GenerateUuid();
        var fields = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "InitialTag"} });
        var changesInit1 = node1.InsertOrUpdate(recordId, fields);
        var changesInit2 = node2.InsertOrUpdate(recordId, fields);

        // Merge initial inserts
        node1.MergeChanges(changesInit2);
        node2.MergeChanges(changesInit1);

        // Node1 updates 'tag'
        var updates1 = CreateFieldsDictionary(new Dictionary<string, string> { {"tag", "Node1UpdatedTag"} });
        var changeUpdate1 = node1.InsertOrUpdate(recordId, updates1);

        // Node2 updates 'tag'
        var updates2 = CreateFieldsDictionary(new Dictionary<string, string> { {"tag", "Node2UpdatedTag"} });
        var changeUpdate2 = node2.InsertOrUpdate(recordId, updates2);

        // Merge changes
        node1.MergeChanges(changeUpdate2);
        node2.MergeChanges(changeUpdate1);

        // Conflict resolved based on site_id (Node2 has higher site_id)
        Assert.That(node1.GetData()[recordId].Fields[new ColName<string>("tag")], Is.EqualTo("Node2UpdatedTag"), "Updates with Conflicts: Tag resolution mismatch");
        Assert.That(node1.GetData(), Is.EqualTo(node2.GetData()), "Updates with Conflicts: Data mismatch");
    }

    [Test]
    public void DeleteAndMerge()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Insert and sync a record
        string recordId = GenerateUuid();
        var fields = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "ToBeDeleted"} });
        var changesInit = node1.InsertOrUpdate(recordId, fields);

        // Merge to node2
        node2.MergeChanges(changesInit);

        // Node1 deletes the record
        var changesDelete = node1.DeleteRecord(recordId);

        // Merge the deletion to node2
        node2.MergeChanges(changesDelete);

        // Both nodes should reflect the deletion
        Assert.That(node1.GetData()[recordId].Fields, Is.Empty, "Delete and Merge: Node1 should have empty fields");
        Assert.That(node2.GetData()[recordId].Fields, Is.Empty, "Delete and Merge: Node2 should have empty fields");
        Assert.That(node1.GetData()[recordId].ColumnVersions.ContainsKey(new ColName<string>("__deleted__")), Is.True, "Delete and Merge: Node1 should have '__deleted__' column version");
        Assert.That(node2.GetData()[recordId].ColumnVersions.ContainsKey(new ColName<string>("__deleted__")), Is.True, "Delete and Merge: Node2 should have '__deleted__' column version");
    }

    // Add more tests here...

    [Test]
    public void CompressChangesNoChanges()
    {
        var changes = new List<Change<string, string>>();
        CRDT<string, string>.CompressChanges(changes);
        Assert.That(changes, Is.Empty, "Compress Changes: No changes should remain after compression.");
    }

    [Test]
    public void CompressSingleChange()
    {
        var changes = new List<Change<string, string>>
        {
            new Change<string, string>("record1", new ColName<string>("col1"), "value1", 1, 1, 1)
        };

        CRDT<string, string>.CompressChanges(changes);

        Assert.That(changes.Count, Is.EqualTo(1), "Compress Changes: Single change should remain unchanged.");
        Assert.That(changes[0].RecordId, Is.EqualTo("record1"), "Compress Changes: Record ID mismatch.");
        Assert.That(changes[0].ColName?.Name, Is.EqualTo("col1"), "Compress Changes: Column name mismatch.");
        Assert.That(changes[0].Value, Is.EqualTo("value1"), "Compress Changes: Value mismatch.");
    }

    [Test]
    public void CompressMultipleChangesOnSameRecordAndColumn()
    {
        var changes = new List<Change<string, string>>
        {
            new Change<string, string>("record1", new ColName<string>("col1"), "old_value", 1, 1, 1),
            new Change<string, string>("record1", new ColName<string>("col1"), "new_value", 2, 2, 1)
        };

        CRDT<string, string>.CompressChanges(changes);

        Assert.That(changes.Count, Is.EqualTo(1), "Compress Changes: Only the latest change should remain.");
        Assert.That(changes[0].Value, Is.EqualTo("new_value"), "Compress Changes: Latest change value mismatch.");
    }

    // Add more compression tests here...

    // Add these tests after the existing tests in the CRDTTests class

    [Test]
    public void TombstoneHandling()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Insert a record and delete it on node1
        string recordId = GenerateUuid();
        var fields = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "Temporary"} });
        var changesInsert = node1.InsertOrUpdate(recordId, fields);
        var changesDelete = node1.DeleteRecord(recordId);

        // Merge changes to node2
        node2.MergeChanges(changesInsert);
        node2.MergeChanges(changesDelete);

        // Node2 tries to insert the same record
        var changesAttemptInsert = node2.InsertOrUpdate(recordId, fields);

        // Merge changes back to node1
        node1.MergeChanges(changesAttemptInsert);

        // Node2 should respect the tombstone
        Assert.That(node2.GetData()[recordId].Fields, Is.Empty, "Tombstone Handling: Node2 should have empty fields");
        Assert.That(node2.GetData()[recordId].ColumnVersions.ContainsKey(new ColName<string>("__deleted__")), Is.True, "Tombstone Handling: Node2 should have '__deleted__' column version");
    }

    [Test]
    public void ConflictResolutionWithSiteId()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Both nodes insert a record with the same id
        string recordId = GenerateUuid();
        var fields1 = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "Node1Tag"} });
        var fields2 = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "Node2Tag"} });
        var changes1 = node1.InsertOrUpdate(recordId, fields1);
        var changes2 = node2.InsertOrUpdate(recordId, fields2);

        // Merge changes
        node1.MergeChanges(changes2);
        node2.MergeChanges(changes1);

        // Both nodes update the 'tag' field multiple times
        var updates1 = CreateFieldsDictionary(new Dictionary<string, string> { {"tag", "Node1Tag1"} });
        var changesUpdate1 = node1.InsertOrUpdate(recordId, updates1);

        updates1 = CreateFieldsDictionary(new Dictionary<string, string> { {"tag", "Node1Tag2"} });
        var changesUpdate2 = node1.InsertOrUpdate(recordId, updates1);

        var updates2 = CreateFieldsDictionary(new Dictionary<string, string> { {"tag", "Node2Tag1"} });
        var changesUpdate3 = node2.InsertOrUpdate(recordId, updates2);

        updates2 = CreateFieldsDictionary(new Dictionary<string, string> { {"tag", "Node2Tag2"} });
        var changesUpdate4 = node2.InsertOrUpdate(recordId, updates2);

        // Merge changes
        node1.MergeChanges(changesUpdate4);
        node2.MergeChanges(changesUpdate2);
        node2.MergeChanges(changesUpdate1);
        node1.MergeChanges(changesUpdate3);

        // Since node2 has a higher site_id, its latest update should prevail
        string expectedTag = "Node2Tag2";

        Assert.That(node1.GetData()[recordId].Fields[new ColName<string>("tag")], Is.EqualTo(expectedTag), "Conflict Resolution: Tag resolution mismatch");
        Assert.That(node1.GetData(), Is.EqualTo(node2.GetData()), "Conflict Resolution: Data mismatch");
    }

    [Test]
    public void LogicalClockUpdate()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Node1 inserts a record
        string recordId = GenerateUuid();
        var fields = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "Node1Tag"} });
        var changesInsert = node1.InsertOrUpdate(recordId, fields);

        // Node2 receives the change
        node2.MergeChanges(changesInsert);

        // Node2's clock should be updated
        Assert.That(node2.GetClock().CurrentTime, Is.GreaterThan(0), "Logical Clock Update: Node2 clock should be greater than 0");
        Assert.That(node2.GetClock().CurrentTime, Is.GreaterThanOrEqualTo(node1.GetClock().CurrentTime),
                    "Logical Clock Update: Node2 clock should be >= Node1 clock");
    }

    [Test]
    public void MergeWithoutConflicts()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Node1 inserts a record
        string recordId1 = GenerateUuid();
        var fields1 = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId1}, {"tag", "Node1Record"} });
        var changes1 = node1.InsertOrUpdate(recordId1, fields1);

        // Node2 inserts a different record
        string recordId2 = GenerateUuid();
        var fields2 = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId2}, {"tag", "Node2Record"} });
        var changes2 = node2.InsertOrUpdate(recordId2, fields2);

        // Merge changes
        node1.MergeChanges(changes2);
        node2.MergeChanges(changes1);

        // Both nodes should have both records
        Assert.That(node1.GetData().ContainsKey(recordId1), Is.True, "Merge without Conflicts: Node1 should contain record_id1");
        Assert.That(node1.GetData().ContainsKey(recordId2), Is.True, "Merge without Conflicts: Node1 should contain record_id2");
        Assert.That(node2.GetData().ContainsKey(recordId1), Is.True, "Merge without Conflicts: Node2 should contain record_id1");
        Assert.That(node2.GetData().ContainsKey(recordId2), Is.True, "Merge without Conflicts: Node2 should contain record_id2");
        Assert.That(node1.GetData(), Is.EqualTo(node2.GetData()), "Merge without Conflicts: Data mismatch between Node1 and Node2");
    }

    [Test]
    public void MultipleMerges()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Node1 inserts a record
        string recordId = GenerateUuid();
        var fields = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"field1", "value1"} });
        var changesInit = node1.InsertOrUpdate(recordId, fields);

        // Merge to node2
        node2.MergeChanges(changesInit);

        // Node2 updates the record
        var updates2 = CreateFieldsDictionary(new Dictionary<string, string> { {"field1", "UpdatedByNode2"} });
        var changesUpdate2 = node2.InsertOrUpdate(recordId, updates2);

        // Node1 updates the record
        var updates1 = CreateFieldsDictionary(new Dictionary<string, string> { {"field1", "UpdatedByNode1"} });
        var changesUpdate1 = node1.InsertOrUpdate(recordId, updates1);

        // Merge changes
        node1.MergeChanges(changesUpdate2);
        node2.MergeChanges(changesUpdate1);

        // Since node2 has a higher site_id, its latest update should prevail
        string expectedTag = "UpdatedByNode2";

        Assert.That(node1.GetData()[recordId].Fields[new ColName<string>("field1")], Is.EqualTo(expectedTag), "Multiple Merges: Tag resolution mismatch");
        Assert.That(node1.GetData(), Is.EqualTo(node2.GetData()), "Multiple Merges: Data mismatch between Node1 and Node2");
    }

    [Test]
    public void InsertingAfterDeletion()
    {
        var node1 = new CRDT<string, string>(1);
        var node2 = new CRDT<string, string>(2);

        // Node1 inserts and deletes a record
        string recordId = GenerateUuid();
        var fields = CreateFieldsDictionary(new Dictionary<string, string> { {"id", recordId}, {"tag", "Temporary"} });
        var changesInsert = node1.InsertOrUpdate(recordId, fields);
        var changesDelete = node1.DeleteRecord(recordId);

        // Merge deletion to node2
        node2.MergeChanges(changesInsert);
        node2.MergeChanges(changesDelete);

        // Node2 tries to insert the same record
        var changesAttemptInsert = node2.InsertOrUpdate(recordId, fields);

        // Merge changes back to node1
        node1.MergeChanges(changesAttemptInsert);

        // The deletion should prevail
        Assert.That(node1.GetData()[recordId].Fields, Is.Empty, "Inserting After Deletion: Node1 should have empty fields");
        Assert.That(node2.GetData()[recordId].Fields, Is.Empty, "Inserting After Deletion: Node2 should have empty fields");
        Assert.That(node1.GetData()[recordId].ColumnVersions.ContainsKey(new ColName<string>("__deleted__")), Is.True,
                    "Inserting After Deletion: Node1 should have '__deleted__' column version");
        Assert.That(node2.GetData()[recordId].ColumnVersions.ContainsKey(new ColName<string>("__deleted__")), Is.True,
                    "Inserting After Deletion: Node2 should have '__deleted__' column version");
    }

    [Test]
    public void ListCRDTTest()
    {
        // Create two replicas
        ListCRDT<string> replicaA = new ListCRDT<string>(replicaId: 1); // Use replicaId 1 for replica A
        ListCRDT<string> replicaB = new ListCRDT<string>(replicaId: 2); // Use replicaId 2 for replica B

        // Replica A inserts "Hello" at index 0
        replicaA.Insert(0, "Hello");
        // Replica A inserts "World" at index 1
        replicaA.Insert(1, "World");

        // Replica B inserts "Goodbye" at index 0
        replicaB.Insert(0, "Goodbye");
        // Replica B inserts "Cruel" at index 1
        replicaB.Insert(1, "Cruel");

        // Print initial states
        Console.Write("Replica A (Visible): ");
        replicaA.PrintVisible(); // Expected: Hello World

        Console.Write("Replica B (Visible): ");
        replicaB.PrintVisible(); // Expected: Goodbye Cruel

        // Merge replicas
        replicaA.Merge(replicaB);
        replicaB.Merge(replicaA);

        // Print merged states
        Console.WriteLine("\nAfter merging:");

        Console.Write("Replica A (Visible): ");
        replicaA.PrintVisible(); // Expected order based on origins and IDs

        Console.Write("Replica B (Visible): ");
        replicaB.PrintVisible(); // Should match Replica A

        // Perform concurrent insertions
        replicaA.Insert(2, "!");
        replicaB.Insert(2, "?");

        // Merge again
        replicaA.Merge(replicaB);
        replicaB.Merge(replicaA);

        // Print after concurrent insertions and merging
        Console.WriteLine("\nAfter concurrent insertions and merging:");

        Console.Write("Replica A (Visible): ");
        replicaA.PrintVisible(); // Expected: Hello Goodbye World ! ?

        Console.Write("Replica B (Visible): ");
        replicaB.PrintVisible(); // Should match Replica A

        // Demonstrate deletions
        replicaA.DeleteElement(1); // Delete "Goodbye"
        replicaB.DeleteElement(0); // Delete "Hello"

        // Merge deletions
        replicaA.Merge(replicaB);
        replicaB.Merge(replicaA);

        // Print states after deletions and merging
        Console.WriteLine("\nAfter deletions and merging:");

        Console.Write("Replica A (Visible): ");
        replicaA.PrintVisible(); // Expected: World ! ?

        Console.Write("Replica B (Visible): ");
        replicaB.PrintVisible(); // Should match Replica A

        // Perform garbage collection
        replicaA.GarbageCollect();
        replicaB.GarbageCollect();

        // Print all elements after garbage collection
        Console.WriteLine("\nAfter garbage collection:");

        Console.WriteLine("Replica A (All Elements):");
        replicaA.PrintAllElements();

        Console.WriteLine("Replica B (All Elements):");
        replicaB.PrintAllElements();

        // Demonstrate delta generation and application
        // Replica A inserts "New Line" at index 2
        replicaA.Insert(2, "New Line");

        // Generate delta from Replica A to Replica B
        var delta = replicaA.GenerateDelta(replicaB);

        // Apply delta to Replica B
        replicaB.ApplyDelta(delta.NewElements, delta.Tombstones);

        // Print final states after delta synchronization
        Console.WriteLine("\nAfter delta synchronization:");

        Console.Write("Replica A (Visible): ");
        replicaA.PrintVisible(); // Expected: World ! New Line ?

        Console.Write("Replica B (Visible): ");
        replicaB.PrintVisible(); // Should match Replica A
    }

    // Add more tests here as needed...
}
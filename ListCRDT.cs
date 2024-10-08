using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace ForSync.CRDT {
  /// <summary>
  /// Represents a unique identifier for a list element.
  /// </summary>
  public struct ElementID : IComparable<ElementID>, IEquatable<ElementID> {
    public ulong ReplicaId { get; set; }
    public ulong Sequence { get; set; }

    public int CompareTo(ElementID other) {
      if (Sequence != other.Sequence)
        return Sequence.CompareTo(other.Sequence);
      return ReplicaId.CompareTo(other.ReplicaId);
    }

    public bool Equals(ElementID other) {
      return ReplicaId == other.ReplicaId && Sequence == other.Sequence;
    }

    public override bool Equals(object obj) {
      return obj is ElementID other && Equals(other);
    }

    public override int GetHashCode() {
      return ReplicaId.GetHashCode() ^ Sequence.GetHashCode();
    }

    public override string ToString() {
      return $"({ReplicaId}, {Sequence})";
    }
  }

  /// <summary>
  /// Comparator for ListElements to establish a total order.
  /// </summary>
  /// <typeparam name="T">Type of the value stored in the list element.</typeparam>
  public class ListElementComparer<T> : IComparer<ListElement<T>> {
    public int Compare(ListElement<T>? a, ListElement<T>? b) {
      if (a == null || b == null)
        throw new ArgumentException("Cannot compare null ListElements.");

      bool aIsRoot = (a.Id.ReplicaId == 0 && a.Id.Sequence == 0);
      bool bIsRoot = (b.Id.ReplicaId == 0 && b.Id.Sequence == 0);

      if (aIsRoot && !bIsRoot)
        return -1;
      if (!aIsRoot && bIsRoot)
        return 1;
      if (aIsRoot && bIsRoot)
        return 0;

      // Compare origin_left
      if (!Nullable.Equals(a.OriginLeft, b.OriginLeft)) {
        if (!a.OriginLeft.HasValue)
          return -1; // a's origin_left is None, so a is first
        if (!b.OriginLeft.HasValue)
          return 1; // b's origin_left is None, so b is first

        int cmp = a.OriginLeft.Value.CompareTo(b.OriginLeft.Value);
        if (cmp != 0)
          return cmp;
      }

      // Compare origin_right
      if (!Nullable.Equals(a.OriginRight, b.OriginRight)) {
        if (!a.OriginRight.HasValue)
          return -1; // a is before
        if (!b.OriginRight.HasValue)
          return 1; // b is before

        int cmp = a.OriginRight.Value.CompareTo(b.OriginRight.Value);
        if (cmp != 0)
          return cmp;
      }

      // If both have the same origins, use ElementID to break the tie
      return a.Id.CompareTo(b.Id);
    }
  }

  /// <summary>
  /// Represents an element in the list CRDT.
  /// </summary>
  /// <typeparam name="T">Type of the value stored in the list element.</typeparam>
  public class ListElement<T> {
    public ElementID Id { get; set; }
    public T? Value { get; set; } // Nullable if T is a reference type
    public ElementID? OriginLeft { get; set; }
    public ElementID? OriginRight { get; set; }

    /// <summary>
    /// Checks if the element is tombstoned (deleted).
    /// </summary>
    public bool IsDeleted => Value != null ? EqualityComparer<T>.Default.Equals(Value, default(T)!) : false;

    public override string ToString() {
      var valueStr = IsDeleted ? "[Deleted]" : $"Value: {Value}";
      var originLeftStr = OriginLeft.HasValue ? OriginLeft.ToString() : "None";
      var originRightStr = OriginRight.HasValue ? OriginRight.ToString() : "None";
      return $"ID: {Id}, {valueStr}, Origin Left: {originLeftStr}, Origin Right: {originRightStr}";
    }
  }

  /// <summary>
  /// Represents the List CRDT using SortedSet.
  /// </summary>
  /// <typeparam name="T">Type of the value stored in the list.</typeparam>
  public class ListCRDT<T> {
    private readonly ulong replicaId; // Unique identifier for the replica
    private ulong counter; // Monotonically increasing counter for generating unique IDs
    private readonly SortedSet<ListElement<T>> elements; // Set of all elements (including tombstoned)
    private readonly Dictionary<ElementID, ListElement<T>> elementIndex; // Maps ElementID to ListElement
    private readonly IComparer<ListElement<T>> comparer;

    /// <summary>
    /// Initializes a new instance of the <see cref="ListCRDT{T}"/> class with a unique replica ID.
    /// </summary>
    /// <param name="replicaId">Unique identifier for the replica.</param>
    public ListCRDT(ulong replicaId) {
      this.replicaId = replicaId;
      this.counter = 0;
      this.comparer = new ListElementComparer<T>();
      this.elements = new SortedSet<ListElement<T>>(comparer);
      this.elementIndex = new Dictionary<ElementID, ListElement<T>>();

      // Initialize with a root element to simplify origins
      ElementID rootId = new ElementID { ReplicaId = 0, Sequence = 0 };
      ListElement<T> rootElement = new ListElement<T> {
        Id = rootId,
        Value = default(T),
        OriginLeft = null,
        OriginRight = null
      };
      elements.Add(rootElement);
      elementIndex[rootId] = rootElement;
    }

    /// <summary>
    /// Inserts a value at the given index.
    /// </summary>
    /// <param name="index">The index at which to insert the value.</param>
    /// <param name="value">The value to insert.</param>
    public void Insert(uint index, T value) {
      ElementID newId = GenerateId();
      ElementID? leftOrigin = null;
      ElementID? rightOrigin = null;

      // Retrieve visible elements (non-tombstoned)
      var visible = GetVisibleElements();
      if (index > visible.Count) {
        index = (uint)visible.Count; // Adjust index if out of bounds
      }

      if (index == 0) {
        // Insert at the beginning, right_origin is the first element
        if (visible.Count > 0) {
          rightOrigin = visible[0].Id;
        }
      } else if (index == visible.Count) {
        // Insert at the end, left_origin is the last element
        if (visible.Count > 0) {
          leftOrigin = visible[visible.Count - 1].Id;
        }
      } else {
        // Insert in the middle
        int prevIndex = (int)index - 1;
        int nextIndex = (int)index;
        if (prevIndex >= 0)
          leftOrigin = visible[prevIndex].Id;
        if (nextIndex < visible.Count)
          rightOrigin = visible[nextIndex].Id;
      }

      // Create a new element with the given value and origins
      ListElement<T> newElement = new ListElement<T> {
        Id = newId,
        Value = value,
        OriginLeft = leftOrigin,
        OriginRight = rightOrigin
      };

      Integrate(newElement);
    }

    /// <summary>
    /// Deletes the element at the given index by tombstoning it.
    /// </summary>
    /// <param name="index">The index of the element to delete.</param>
    public void DeleteElement(uint index) {
      var visible = GetVisibleElements();
      if (index >= visible.Count)
        return; // Index out of bounds, do nothing

      ElementID targetId = visible[(int)index].Id;
      if (elementIndex.TryGetValue(targetId, out var existingElement)) {
        // Tombstone the element by setting its value to default
        ListElement<T> updated = new ListElement<T> {
          Id = existingElement.Id,
          Value = default(T),
          OriginLeft = existingElement.OriginLeft,
          OriginRight = existingElement.OriginRight
        };

        elements.Remove(existingElement);
        elements.Add(updated);
        elementIndex[targetId] = updated;
      }
    }

    /// <summary>
    /// Merges another <see cref="ListCRDT{T}"/> into this one.
    /// </summary>
    /// <param name="other">The other <see cref="ListCRDT{T}"/> to merge.</param>
    public void Merge(ListCRDT<T> other) {
      foreach (var elem in other.elements) {
        if (elem.Id.ReplicaId == 0 && elem.Id.Sequence == 0)
          continue; // Skip the root element

        Integrate(elem);
      }
    }

    /// <summary>
    /// Generates a delta containing operations not seen by the other replica.
    /// </summary>
    /// <param name="other">The other <see cref="ListCRDT{T}"/> to compare against.</param>
    /// <returns>A tuple containing new elements and tombstones.</returns>
    public (List<ListElement<T>> NewElements, List<ElementID> Tombstones) GenerateDelta(ListCRDT<T> other) {
      List<ListElement<T>> newElements = new List<ListElement<T>>();
      List<ElementID> tombstones = new List<ElementID>();

      foreach (var elem in elements) {
        if (elem.Id.ReplicaId == 0 && elem.Id.Sequence == 0)
          continue; // Skip the root element

        if (!other.HasElement(elem.Id)) {
          newElements.Add(elem);
          if (elem.IsDeleted)
            tombstones.Add(elem.Id);
        }
      }

      return (newElements, tombstones);
    }

    /// <summary>
    /// Applies a delta to this CRDT.
    /// </summary>
    /// <param name="newElements">The new elements to integrate.</param>
    /// <param name="tombstones">The tombstones to apply.</param>
    public void ApplyDelta(List<ListElement<T>> newElements, List<ElementID> tombstones) {
      // Apply insertions
      foreach (var elem in newElements) {
        if (elem.Id.ReplicaId == 0 && elem.Id.Sequence == 0)
          continue; // Skip the root element

        var existing = FindElement(elem.Id);
        if (existing == null) {
          Integrate(elem);
        } else {
          if (elem.IsDeleted) {
            // Update tombstone
            ListElement<T> updated = new ListElement<T> {
              Id = existing.Id,
              Value = default(T),
              OriginLeft = existing.OriginLeft,
              OriginRight = existing.OriginRight
            };
            elements.Remove(existing);
            elements.Add(updated);
            elementIndex[updated.Id] = updated;
          }
        }
      }

      // Apply tombstones
      foreach (var id in tombstones) {
        var existing = FindElement(id);
        if (existing != null) {
          // Update tombstone
          ListElement<T> updated = new ListElement<T> {
            Id = existing.Id,
            Value = default(T),
            OriginLeft = existing.OriginLeft,
            OriginRight = existing.OriginRight
          };
          elements.Remove(existing);
          elements.Add(updated);
          elementIndex[id] = updated;
        }
      }
    }

    /// <summary>
    /// Retrieves the current list as a list of values.
    /// </summary>
    /// <returns>A list of current values in the CRDT.</returns>
    public List<T> GetValues() {
      return elements
          .Where(e => e.Id.ReplicaId != 0 || e.Id.Sequence != 0)
          .Where(e => !e.IsDeleted)
          .Select(e => e.Value!)
          .ToList();
    }

    /// <summary>
    /// Prints the current visible list for debugging.
    /// </summary>
    public void PrintVisible() {
      foreach (var elem in elements) {
        if (elem.Id.ReplicaId == 0 && elem.Id.Sequence == 0)
          continue; // Skip the root element

        if (!elem.IsDeleted)
          Console.Write($"{elem.Value} ");
      }
      Console.WriteLine();
    }

    /// <summary>
    /// Prints all elements including tombstones for debugging.
    /// </summary>
    public void PrintAllElements() {
      foreach (var elem in elements) {
        Console.WriteLine(elem);
      }
    }

    /// <summary>
    /// Performs garbage collection by removing tombstones that are safe to delete.
    /// </summary>
    public void GarbageCollect() {
      var toRemove = elements.Where(e => e.IsDeleted && e.Id.ReplicaId != 0).ToList();
      foreach (var elem in toRemove) {
        elements.Remove(elem);
        elementIndex.Remove(elem.Id);
      }
    }

    #region Private Methods

    /// <summary>
    /// Generates a unique <see cref="ElementID"/>.
    /// </summary>
    /// <returns>A new unique <see cref="ElementID"/>.</returns>
    private ElementID GenerateId() {
      return new ElementID {
        ReplicaId = replicaId,
        Sequence = ++counter
      };
    }

    /// <summary>
    /// Checks if an element exists by its ID.
    /// </summary>
    /// <param name="id">The <see cref="ElementID"/> to check.</param>
    /// <returns><c>true</c> if the element exists; otherwise, <c>false</c>.</returns>
    private bool HasElement(ElementID id) {
      return elementIndex.ContainsKey(id);
    }

    /// <summary>
    /// Finds an element by its ID.
    /// </summary>
    /// <param name="id">The <see cref="ElementID"/> of the element to find.</param>
    /// <returns>The found <see cref="ListElement{T}"/>, or <c>null</c> if not found.</returns>
    private ListElement<T>? FindElement(ElementID id) {
      if (elementIndex.TryGetValue(id, out var elem))
        return elem;
      return null;
    }

    /// <summary>
    /// Retrieves visible (non-tombstoned) elements in order.
    /// </summary>
    /// <returns>A list of visible <see cref="ListElement{T}"/> instances.</returns>
    private List<ListElement<T>> GetVisibleElements() {
      return elements
          .Where(e => e.Id.ReplicaId != 0 || e.Id.Sequence != 0)
          .Where(e => !e.IsDeleted)
          .ToList();
    }

    /// <summary>
    /// Integrates a single element into the CRDT.
    /// </summary>
    /// <param name="newElem">The new <see cref="ListElement{T}"/> to integrate.</param>
    private void Integrate(ListElement<T> newElem) {
      if (elementIndex.ContainsKey(newElem.Id)) {
        var existing = FindElement(newElem.Id);
        if (existing != null && newElem.IsDeleted) {
          // Update tombstone
          ListElement<T> updated = new ListElement<T> {
            Id = existing.Id,
            Value = default(T),
            OriginLeft = existing.OriginLeft,
            OriginRight = existing.OriginRight
          };
          elements.Remove(existing);
          elements.Add(updated);
          elementIndex[updated.Id] = updated;
        }
        return;
      }

      // Insert the new element
      elements.Add(newElem);
      elementIndex[newElem.Id] = newElem;
    }

    #endregion
  }
}

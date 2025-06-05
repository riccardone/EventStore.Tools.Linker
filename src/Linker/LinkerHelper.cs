using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using KurrentDB.Client;

namespace Linker;

public class LinkerHelper
{
    public bool IsValidForReplica(string eventType, string eventStreamId, Position? originalPosition, string positionEventType, IFilterService filterService)
    {
        if (eventType == null)
            return false;
        if (eventStreamId.StartsWith("$"))
            return false;
        if (eventType.StartsWith("$") && !eventType.StartsWith("$$$"))
            return false;
        if (eventType.Equals(positionEventType) ||
            !originalPosition.HasValue)
            return false;
        if (filterService != null && !filterService.IsValid(eventType, eventStreamId))
            return false;
        return true;
    }

    public bool TryProcessMetadata(string streamId, StreamPosition eventNumber, DateTime created, string origin, IDictionary<string, JsonNode?> inputMetadata, out IDictionary<string, JsonNode> outputMetadata)
    {
        outputMetadata = null;

        if (inputMetadata.ContainsKey("$local"))
            return false;

        // We don't want to replicate an event back to any of its origins
        if (inputMetadata.ContainsKey("$origin"))
        {
            string[] origins = inputMetadata["$origin"].ToString().Split(',');
            if (origins.Any(o => o.Equals(origin)))
                return false;
        }

        outputMetadata = EnrichMetadata(streamId, eventNumber.ToInt64(), created, inputMetadata, origin);
        return true;
    }

    private IDictionary<string, JsonNode> EnrichMetadata(string streamId, long eventNumber, DateTime created, IDictionary<string, JsonNode> metadata, string origin)
    {
        if (metadata.ContainsKey("$origin"))
            // This node is part of a replica chain and therefore we don't want to forget the previous origins
            metadata["$origin"] = $"{metadata["$origin"]},{origin}";
        else
            metadata.Add("$origin", origin);

        if (!metadata.ContainsKey("$applies"))
            metadata.Add("$applies", created.ToString("o"));

        if (!metadata.ContainsKey("$eventStreamId"))
            metadata.Add("$eventStreamId", streamId);

        if (!metadata.ContainsKey("$eventNumber"))
            metadata.Add("$eventNumber", eventNumber);

        return metadata;
    }

    public int CalculateSpeed(int currentCountPerSec, int previousCountPerSec)
    {
        if (currentCountPerSec > 0 && previousCountPerSec == 0)
            return 0;
        if (previousCountPerSec == 0)
            return 0;
        return currentCountPerSec - previousCountPerSec;
    }

    public IDictionary<string, JsonNode?> DeserializeObject(ReadOnlyMemory<byte> obj)
    {
        if (obj.IsEmpty)
            return new Dictionary<string, JsonNode?>();

        var json = Encoding.UTF8.GetString(obj.Span);
        var jsonObject = JsonNode.Parse(json)?.AsObject();

        return jsonObject is not null
            ? new Dictionary<string, JsonNode?>(jsonObject)
            : new Dictionary<string, JsonNode?>();
    }

    public byte[] SerializeObject(object obj)
    {
        var json = JsonSerializer.Serialize(obj);
        return Encoding.UTF8.GetBytes(json);
    }
}
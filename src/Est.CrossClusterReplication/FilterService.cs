using System.Collections.Generic;
using System.Linq;
using Est.CrossClusterReplication.Contracts;
using Est.CrossClusterReplication.Model;

namespace Est.CrossClusterReplication
{
    public class FilterService : IFilterService
    {
        private readonly IDictionary<FilterOperation, List<ReplicaFilter>> _filters;

        public FilterService(IEnumerable<ReplicaFilter> filters)
        {
            _filters = new Dictionary<FilterOperation, List<ReplicaFilter>>();
            if (filters == null)
                return;
            foreach (var replicaFilter in filters)
            {
                if (_filters.ContainsKey(replicaFilter.FilterOperation))
                {
                    _filters[replicaFilter.FilterOperation].Add(replicaFilter);
                }
                else
                {
                    _filters.Add(replicaFilter.FilterOperation, new List<ReplicaFilter> { replicaFilter });
                }
            }
        }

        public bool IsValid(string eventType, string eventStreamId)
        {
            if (!_filters.Any())
                return true;
            if (_filters.ContainsKey(FilterOperation.Exclude) &&
                IsExcludedByFilters(eventType, eventStreamId, _filters[FilterOperation.Exclude]))
                return false;
            if (_filters.ContainsKey(FilterOperation.Include))
                return IsIncludedByFilters(eventType, eventStreamId, _filters[FilterOperation.Include]);
            return false;
        }

        private static bool IsIncludedByFilters(string eventType, string eventStreamId, List<ReplicaFilter> filters)
        {
            if (filters == null || !filters.Any())
                return true;

            var includedByStreamFilters = true;
            var includedByEventTypeFilters = true;

            foreach (var replicaFilter in filters)
            {
                switch (replicaFilter.FilterType)
                {
                    case FilterType.Stream:
                        includedByStreamFilters = IsIncludedByStream(eventStreamId, replicaFilter);
                        break;
                    case FilterType.EventType:
                        includedByEventTypeFilters = IsIncludedByEventType(eventType, replicaFilter);
                        break;
                }
            }
            
            return includedByStreamFilters && includedByEventTypeFilters;
        }

        private static bool IsExcludedByFilters(string eventType, string eventStreamId, List<ReplicaFilter> filters)
        {
            if (filters == null || !filters.Any())
                return false;

            var excludedByStreamFilters = false;
            var excludedByEventTypeFilters = false;

            foreach (var replicaFilter in filters)
            {
                switch (replicaFilter.FilterType)
                {
                    case FilterType.Stream:
                        excludedByStreamFilters = IsExcludedByStream(eventStreamId, replicaFilter);
                        break;
                    case FilterType.EventType:
                        excludedByEventTypeFilters = IsExcludedByEventType(eventType, replicaFilter);
                        break;
                }
            }
            return excludedByStreamFilters || excludedByEventTypeFilters;
        }

        private static bool IsExcludedByEventType(string eventType, ReplicaFilter replicaFilter)
        {
            if (replicaFilter.Value.EndsWith("*"))
            {
                if (eventType.StartsWith(replicaFilter.Value.TrimEnd('*')))
                {
                    return true;
                }
            }
            else
            {
                if (eventType.Equals(replicaFilter.Value))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsIncludedByEventType(string eventType, ReplicaFilter replicaFilter)
        {
            if (replicaFilter.Value.EndsWith("*"))
            {
                if (eventType.StartsWith(replicaFilter.Value.TrimEnd('*')))
                {
                    return true;
                }
            }
            else
            {
                if (eventType.Equals(replicaFilter.Value))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsExcludedByStream(string eventStreamId, ReplicaFilter replicaFilter)
        {
            if (replicaFilter.Value.EndsWith("*"))
            {
                if (eventStreamId.StartsWith(replicaFilter.Value.TrimEnd('*')))
                {
                    return true;
                }
            }
            else
            {
                if (eventStreamId.Equals(replicaFilter.Value))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsIncludedByStream(string eventStreamId, ReplicaFilter replicaFilter)
        {
            if (replicaFilter.Value.EndsWith("*"))
            {
                if (eventStreamId.StartsWith(replicaFilter.Value.TrimEnd('*')))
                {
                    return true;
                }
            }
            else
            {
                if (eventStreamId.Equals(replicaFilter.Value))
                {
                    return true;
                }
            }

            return false;
        }
    }
}

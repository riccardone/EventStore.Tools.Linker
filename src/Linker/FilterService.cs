using System.Collections.Generic;
using System.Linq;

namespace Linker
{
    public class FilterService : IFilterService
    {
        private readonly IDictionary<FilterOperation, List<Filter>> _filters;

        public FilterService(params Filter[] filters) :this(filters.AsEnumerable()){}

        public FilterService(IEnumerable<Filter> filters)
        {
            _filters = new Dictionary<FilterOperation, List<Filter>>();
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
                    _filters.Add(replicaFilter.FilterOperation, new List<Filter> { replicaFilter });
                }
            }
        }

        public bool IsValid(string eventType, string eventStreamId)
        {
            if (!_filters.Any())
                return true;

            var shouldBeExcluded = _filters.ContainsKey(FilterOperation.Exclude) && IsExcludedByFilters(eventType, eventStreamId, _filters[FilterOperation.Exclude]);
            var shouldBeIncluded = !_filters.ContainsKey(FilterOperation.Include) || IsIncludedByFilters(eventType, eventStreamId, _filters[FilterOperation.Include]);

            return !shouldBeExcluded && shouldBeIncluded;
        }

        private static bool IsIncludedByFilters(string eventType, string eventStreamId, List<Filter> filters)
        {
            if (filters == null || !filters.Any())
                return true;

            var shouldIncludeByStream = filters.Any(f => f.FilterType == FilterType.Stream && IsIncludedByStream(eventStreamId, f));
            var shouldIncludeByEventType = filters.Any(f => f.FilterType == FilterType.EventType && IsIncludedByEventType(eventType, f));

            return shouldIncludeByEventType || shouldIncludeByStream;
        }

        private static bool IsExcludedByFilters(string eventType, string eventStreamId, List<Filter> filters)
        {
            if (filters == null || !filters.Any())
                return false;

            var shouldExcludeByStream = filters.Any(f => f.FilterType == FilterType.Stream && IsExcludedByStream(eventStreamId, f));
            var shouldExcludeByEventType = filters.Any(f => f.FilterType == FilterType.EventType && IsExcludedByEventType(eventType, f));

            return shouldExcludeByStream || shouldExcludeByEventType;
        }

        private static bool IsExcludedByEventType(string eventType, Filter replicaFilter)
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

        private static bool IsIncludedByEventType(string eventType, Filter replicaFilter)
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

        private static bool IsExcludedByStream(string eventStreamId, Filter replicaFilter)
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

        private static bool IsIncludedByStream(string eventStreamId, Filter replicaFilter)
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

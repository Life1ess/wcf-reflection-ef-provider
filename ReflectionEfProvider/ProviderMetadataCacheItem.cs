using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Linq;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Use this class to cache metadata for providers.
    /// </summary>
    public class ProviderMetadataCacheItem
    {
        /// <summary>
        /// list of top level entity sets
        /// </summary>
        private readonly Dictionary<string, ResourceSet> _entitySets;

        /// <summary>
        /// Collection of service operations, keyed by name.
        /// </summary>
        private readonly Dictionary<string, ServiceOperation> _serviceOperations;

        /// <summary>
        /// Target type for the data provider.
        /// </summary>
        private readonly Type _type;

        /// <summary>
        /// Cache of resource properties per type.
        /// </summary>
        private readonly Dictionary<Type, ResourceTypeCacheItem> _typeCache;

        /// <summary>
        /// Cache of immediate derived types per type.
        /// </summary>
        private readonly Dictionary<ResourceType, List<ResourceType>> _childTypesCache;

        /// <summary>
        /// Cache of IL's instructions for getting the query root for sets.
        /// </summary>
        private readonly Dictionary<ResourceSet, Func<object, IQueryable>> _queryRootCache;

        /// <summary>
        /// Collection of service operations, keyed by name.
        /// </summary>
        internal Dictionary<string, ServiceOperation> ServiceOperations
        {
            get { return _serviceOperations; }
        }

        /// <summary>
        /// Cache of ResourceTypeCacheItems which contains the ResourceType and its metadata.
        /// </summary>
        internal IEnumerable<ResourceTypeCacheItem> ResourceTypeCacheItems
        {
            get { return _typeCache.Values; }
        }

        /// <summary>
        /// Cache of immediate derived types per type.
        /// </summary>
        internal Dictionary<ResourceType, List<ResourceType>> ChildTypesCache
        {
            get { return _childTypesCache; }
        }

        /// <summary>
        /// list of top level entity sets
        /// </summary>
        internal Dictionary<string, ResourceSet> EntitySets
        {
            get { return _entitySets; }
        }

        /// <summary>
        /// Target type for the data provider.
        /// </summary>
        internal Type Type
        {
            get { return _type; }
        }

        /// <summary>
        /// Returns the cache of IL's instructions for getting the query root for sets.
        /// </summary>
        internal Dictionary<ResourceSet, Func<object, IQueryable>> QueryRootCache
        {
            get { return _queryRootCache; }
        }

        /// <summary>
        /// Initializes a new <see cref="T:System.Data.Services.Caching.ProviderMetadataCacheItem"/> instance.
        /// </summary>
        /// <param name="type">Type of data context for which metadata will be generated.</param>
        internal ProviderMetadataCacheItem(Type type)
        {
            _serviceOperations = new Dictionary<string, ServiceOperation>(EqualityComparer<string>.Default);
            _typeCache = new Dictionary<Type, ResourceTypeCacheItem>(EqualityComparer<Type>.Default);
            _entitySets = new Dictionary<string, ResourceSet>(EqualityComparer<string>.Default);
            _childTypesCache = new Dictionary<ResourceType, List<ResourceType>>(ReferenceEqualityComparer<ResourceType>.Instance);
            _queryRootCache = new Dictionary<ResourceSet, Func<object, IQueryable>>(ReferenceEqualityComparer<ResourceSet>.Instance);
            _type = type;
        }

        /// <summary>
        /// Gets the ResourceType for the given CLR type.
        /// 
        /// </summary>
        /// <param name="type">CLR type.</param>
        /// <returns>
        /// ResourceType instance for the given CLR type.
        /// </returns>
        internal ResourceType TryGetResourceType(Type type)
        {
            ResourceTypeCacheItem resourceTypeCacheItem = TryGetResourceTypeCacheItem(type);

            if (resourceTypeCacheItem == null)
            {
                return null;
            }
            
            return resourceTypeCacheItem.ResourceType;
        }

        /// <summary>
        /// Gets the ResourceType for the given CLR type.
        /// 
        /// </summary>
        /// <param name="type">CLR type.</param>
        /// <returns>
        /// ResourceType instance for the given CLR type.
        /// </returns>
        internal ResourceTypeCacheItem TryGetResourceTypeCacheItem(Type type)
        {
            ResourceTypeCacheItem resourceTypeCacheItem;
            _typeCache.TryGetValue(type, out resourceTypeCacheItem);
            return resourceTypeCacheItem;
        }

        /// <summary>
        /// Adds the given ResourceType to the cache.
        /// 
        /// </summary>
        /// <param name="type">CLR type.</param>
        /// <param name="resourceType">ResourceType instance.</param>
        internal void AddResourceType(Type type, ResourceType resourceType)
        {
            var resourceTypeCacheItem = new ResourceTypeCacheItem(resourceType);
            _typeCache.Add(type, resourceTypeCacheItem);
        }
    }
}
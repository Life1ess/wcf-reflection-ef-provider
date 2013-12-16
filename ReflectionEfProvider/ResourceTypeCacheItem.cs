using System;
using System.Collections.Generic;
using System.Data.Services;
using System.Data.Services.Providers;
using System.Reflection;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Class to cache information for the given resource type.
    /// 
    /// </summary>
    public class ResourceTypeCacheItem
    {
        /// <summary>
        /// Cache for storing the metadata about the property.
        /// </summary>
        private readonly Dictionary<ResourceProperty, ResourcePropertyCacheItem> _resourcePropertyMetadataCache = new Dictionary<ResourceProperty, ResourcePropertyCacheItem>(ReferenceEqualityComparer<ResourceProperty>.Instance);
        
        /// <summary>
        /// ResourceType instance for which the metadata needs to be cached.
        /// </summary>
        private readonly ResourceType _resourceType;

        /// <summary>
        /// Constructor Delegate for the resource type.
        /// </summary>
        private Func<object> _constructorDelegate;

        /// <summary>
        /// Cached delegate to create a new instance of this type.
        /// </summary>
        internal Func<object> ConstructorDelegate
        {
            get
            {
                if (_constructorDelegate == null)
                {
                    _constructorDelegate = (Func<object>)CommonUtil.CreateNewInstanceConstructor(_resourceType.InstanceType, _resourceType.FullName, typeof(object));
                }

                return _constructorDelegate;
            }
        }

        /// <summary>
        /// Gets the instance of ResourceType whose metadata is getting cached in this cache item.
        /// </summary>
        internal ResourceType ResourceType
        {
            get { return _resourceType; }
        }

        /// <summary>
        /// Creates a new instance of ResourceTypeCacheItem.
        /// 
        /// </summary>
        /// <param name="resourceType">ResourceType instance.</param>
        public ResourceTypeCacheItem(ResourceType resourceType)
        {
            _resourceType = resourceType;
        }

        /// <summary>
        /// Gets the cache item for the given property.
        /// </summary>
        /// <returns>
        /// the cache item for the given property.
        /// </returns>
        internal ResourcePropertyCacheItem GetResourcePropertyCacheItem(Type ownerType, ResourceProperty property)
        {
            ResourcePropertyCacheItem propertyCacheItem;
            _resourcePropertyMetadataCache.TryGetValue(property, out propertyCacheItem);

            if (propertyCacheItem == null)
            {
                PropertyInfo propertyInfo = ownerType.GetProperty(property.Name);

                if (propertyInfo == null)
                {
                    throw new DataServiceException(500, string.Format("Public property {1} not defined on type {0}", _resourceType.FullName, property.Name));
                }

                propertyCacheItem = new ResourcePropertyCacheItem(propertyInfo);
                _resourcePropertyMetadataCache.Add(property, propertyCacheItem);
            }

            return propertyCacheItem;
        }

        /// <summary>
        /// Add the given property metadata to the cache.
        /// 
        /// </summary>
        /// <param name="property">ResourceProperty instance.</param>
        /// <param name="propertyCacheItem">Cache item containing the metadata about the property.</param>
        internal void AddResourcePropertyCacheItem(ResourceProperty property, ResourcePropertyCacheItem propertyCacheItem)
        {
            _resourcePropertyMetadataCache.Add(property, propertyCacheItem);
        }
    }
}
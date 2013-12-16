using System;
using System.Collections.Generic;
using System.Threading;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Use this class to cache metadata through ProviderMetadataCacheItem instances.
    /// 
    /// </summary>
    /// <typeparam name="T">Type of the item to be cached.</typeparam>
    internal static class MetadataCache<T>
    {
        /// <summary>
        /// AppDomain-wide cache for metadata items.
        /// </summary>
        private static readonly Dictionary<MetadataCacheKey, T> Cache = new Dictionary<MetadataCacheKey, T>(new MetadataCacheKey.Comparer());
        
        /// <summary>
        /// Reader/writer lock for AppDomain <see cref="F:System.Data.Services.Caching.MetadataCache`1.cache"/>.
        /// </summary>
        private static readonly ReaderWriterLockSlim CacheLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        /// <summary>
        /// Adds a new cache item, and returns the item that is put in the cache.
        /// </summary>
        /// <param name="serviceType">Type of service with metadata being cached.</param>
        /// <param name="item">Item being added.</param>
        /// <returns>
        /// The item being put in the cache (possibly an existing one).
        /// </returns>
        /// 
        /// <remarks>
        /// This method is thread-safe but not re-entrant.
        /// </remarks>
        internal static T AddCacheItem(Type serviceType, T item)
        {
            var key = new MetadataCacheKey(serviceType);
            CacheLock.EnterWriteLock();
            T obj;

            try
            {
                if (!Cache.TryGetValue(key, out obj))
                {
                    Cache.Add(key, item);
                    obj = item;
                }
            }
            finally
            {
                CacheLock.ExitWriteLock();
            }

            return obj;
        }

        /// <summary>
        /// Tries to look up metadata for the specifed service type and context instance.
        /// </summary>
        /// <param name="serviceType">Type of service with metadata being cached.</param>
        /// <returns>
        /// The cached metadata item, if one exists.
        /// </returns>
        /// 
        /// <remarks>
        /// This method is thread-safe but not re-entrant.
        /// </remarks>
        internal static T TryLookup(Type serviceType)
        {
            var key = new MetadataCacheKey(serviceType);
            CacheLock.EnterReadLock();
            T obj;

            try
            {
                Cache.TryGetValue(key, out obj);
            }
            finally
            {
                CacheLock.ExitReadLock();
            }

            return obj;
        }

        /// <summary>
        /// This type is used as the key in the metadata cache.
        /// </summary>
        internal struct MetadataCacheKey
        {
            /// <summary>
            /// Connection string used to segment service type.
            /// </summary>
            private readonly string _dataContextConnection;

            /// <summary>
            /// Hash code for this instance.
            /// </summary>
            private readonly int _hashCode;

            /// <summary>
            /// Service type.
            /// </summary>
            private readonly Type _serviceType;

            /// <summary>
            /// Initializes a new MetadataCacheKey instance.
            /// </summary>
            /// <param name="serviceType">Service type for key.</param>
            internal MetadataCacheKey(Type serviceType)
            {
                _serviceType = serviceType;
                _dataContextConnection = null;
                _hashCode = _serviceType.GetHashCode();                
            }

            /// <summary>
            /// Comparer for metadata cache keys.
            /// </summary>
            internal class Comparer : IEqualityComparer<MetadataCacheKey>
            {
                /// <summary>
                /// Compares the specified keys.
                /// </summary>
                /// <param name="x">First key.</param><param name="y">Second key.</param>
                /// <returns>
                /// true if <paramref name="x"/> equals <paramref name="y"/>, false otherwise.
                /// </returns>
                public bool Equals(MetadataCacheKey x, MetadataCacheKey y)
                {
                    if (x._dataContextConnection != y._dataContextConnection)
                    {
                        return false;
                    }

                    return x._serviceType == y._serviceType;
                }

                /// <summary>
                /// Gets the hash code for the object.
                /// </summary>
                /// <param name="obj">Object.</param>
                /// <returns>
                /// The hash code for this key.
                /// </returns>
                public int GetHashCode(MetadataCacheKey obj)
                {
                    return obj._hashCode;
                }
            }
        }
    }
}
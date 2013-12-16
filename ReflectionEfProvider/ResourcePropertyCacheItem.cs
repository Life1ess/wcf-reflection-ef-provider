using System.Reflection;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Class for storing metadata for a given ResourceProperty.
    /// 
    /// </summary>
    public class ResourcePropertyCacheItem
    {
        /// <summary>
        /// PropertyInfo instance for the given ResourceProperty.
        /// </summary>
        private readonly PropertyInfo _propertyInfo;

        /// <summary>
        /// Returns PropertyInfo instance for the given ResourceProperty.
        /// 
        /// </summary>
        public PropertyInfo PropertyInfo
        {
            get { return _propertyInfo; }
        }

        /// <summary>
        /// Creates a new instance of ResourcePropertyCacheItem.
        /// 
        /// </summary>
        /// <param name="propertyInfo">PropertyInfo instance for the given ResourceProperty.</param>
        internal ResourcePropertyCacheItem(PropertyInfo propertyInfo)
        {
            _propertyInfo = propertyInfo;
        }
    }
}
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Services;
using System.Data.Services.Common;
using System.Data.Services.Providers;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Xml;

namespace ReflectionEfProvider
{
    public class ReflectionEntityFrameworkDataServiceProvider : IDataServiceMetadataProvider, IDataServiceQueryProvider, IDataServiceProviderBehavior, IServiceProvider, IDisposable
    {
        private const string CollectionOfCollectionProperty = "The property '{0}' on a type '{1}' is not a valid property. A collection property that contains collection types is not supported.";
        private const string CollectionOfUnsupportedTypeProperty = "The property '{0}' on type '{1}' is a collection property with unsupported item type '{2}'. Only primitive types and complex types are valid item types for a collection property.";
        private const string ComplexTypeWithNavigationProperty = "The property '{0}' on a complex type '{1}' is not a valid property. Navigation properties are not supported on complex types.";
        private const string EntityPropertyWithNoEntitySet = "Type '{0}' has property '{1}' of entity type. Either this property has no corresponding entity set in the data context or one of its inherited types has a corresponding entity set. Specify IgnoreProperties attribute on the entity type for this property or use a property type that has a corresponding entity set in the data context.";
        private const string ETagPropertyNameNotValid = "The property name'{0}' specified in the ETagAttribute on type '{1}' is not a valid property name. Please specify a valid property name.";
        private const string InvalidEntitySetProperty = "On data context type '{1}', there is a top IQueryable property '{0}' whose element type is not an entity type. Make sure that the IQueryable property is of entity type or specify the IgnoreProperties attribute on the data context type to ignore this property.";
        private const string InvalidProperty = "The property '{0}' on type '{1}' is not a valid property. Make sure that the type of the property is a public type and a supported primitive type or a entity type with a valid key or a complex type.";
        private const string InvalidTypeSpecified = "Internal Server Error. The type '{0}' is not a complex type or an entity type.";
        private const string KeyPropertiesCannotBeIgnored = "Key properties cannot be ignored using the IgnoreProperties attribute. For type '{0}', please make sure that there is a key property which is not ignored.";
        private const string MultipleEntitySetsForSameType = "Property '{0}' and '{1}' are IQueryable of types '{2}' and '{3}' and type '{2}' is an ancestor for type '{3}'. Please make sure that there is only one IQueryable property for each type hierarchy.";
        private const string OverloadingNotSupported = "Overloading is not supported but type '{0}' has an overloaded method '{1}'.";
        private const string PropertyMustBeNavigationPropertyOnType = "The resource property '{0}' must be a navigation property on the resource type '{1}'.";
        private const string ResourceTypeHasNoPublicallyVisibleProperties = "The CLR Type '{0}' has no public properties and is not a supported resource type.";
        private const string ResourceTypeMustBeDeclaringTypeForProperty = "The resource type '{0}' must contain the resource property instance '{1}'.";
        private const string UnknownResourceSet = "The resource set '{0}' is not known to the provider.";
        private const string UnknownResourceType = "The resource type '{0}' is not known to the provider.";
        private const string UnknownResourceTypeInstance = "The given resource type instance for the type '{0}' is not known to the metadata provider.";
        private const string UnsupportedType = "Internal Server Error. The type '{0}' is not supported.";
        private const string UnsupportedPropertyType = "Internal Server Error. The property '{0}' is of type '{1}' which is an unsupported type.";

        private static readonly MethodInfo RemoveKeyPropertiesMethod = typeof(ResourceType).GetMethod("RemoveKeyProperties", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly MethodInfo IsAssignableFromMethod = typeof(ResourceType).GetMethod("IsAssignableFrom", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly ProviderBehavior EntityFrameworkProviderBehavior = new ProviderBehavior(ProviderQueryBehaviorKind.EntityFrameworkProviderQueryBehavior);        
        private readonly ICollection<Type> _serviceInterfaces;
        private object _dataServiceInstance;
        private object _dataSourceInstance;
        private readonly IServiceProvider _serviceProvider;
        private ProviderMetadataCacheItem _metadata;
        private bool _metadataRequiresInitialization;

        /// <summary>
        /// Initializes a new ReflectionEntityFrameworkDataServiceProvider instance.
        /// </summary>
        /// <param name="dataServiceInstance">data service instance.</param>
        /// <param name="dataSourceInstance">data source instance.</param>
        /// <param name="serviceProvider">service provider</param>
        /// <param name="serviceInterfaces">list of services to query for additional service operations</param>
        public ReflectionEntityFrameworkDataServiceProvider(object dataServiceInstance, object dataSourceInstance, IServiceProvider serviceProvider = null, ICollection<Type> serviceInterfaces = null)
        {
            if (dataServiceInstance == null)
            {
                throw new ArgumentNullException("dataServiceInstance");
            }

            if (dataSourceInstance == null)
            {
                throw new ArgumentNullException("dataSourceInstance");
            }

            _dataServiceInstance = dataServiceInstance;
            _dataSourceInstance = dataSourceInstance;
            _serviceProvider = serviceProvider;
            _serviceInterfaces = serviceInterfaces;
        }

        #region IDisposable members

        void IDisposable.Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region IDataServiceMetadataProvider members

        /// <summary>
        /// Name of the EDM container
        /// </summary>
        public string ContainerName
        {
            get { return DataSourceType.Name; }
        }

        /// <summary>
        /// Namespace name for the EDM container.
        /// </summary>
        public string ContainerNamespace
        {
            get { return DataSourceType.Namespace; }
        }

        /// <summary>
        /// The method must return a collection of all the types derived from <paramref name="resourceType"/>.
        ///             The collection returned should NOT include the type passed in as a parameter.
        ///             An implementer of the interface should return null if the type does not have any derived types (ie. null == no derived types).
        /// </summary>
        /// <param name="resourceType">Resource to get derived resource types from.</param>
        /// <returns>
        /// A collection of resource types (<see cref="T:System.Data.Services.Providers.ResourceType"/>) derived from the specified <paramref name="resourceType"/>
        ///             or null if there no types derived from the specified <paramref name="resourceType"/> exist.
        /// </returns>
        public IEnumerable<ResourceType> GetDerivedTypes(ResourceType resourceType)
        {
            if (resourceType == null)
            {
                throw new ArgumentNullException("resourceType");
            }

            if (!ChildTypesCache.ContainsKey(resourceType))
            {
                throw new InvalidOperationException(string.Format(UnknownResourceTypeInstance, resourceType.FullName));
            }

            List<ResourceType> childTypes = ChildTypesCache[resourceType];

            if (childTypes != null)
            {
                foreach (ResourceType resourceType1 in childTypes)
                {
                    yield return resourceType1;

                    foreach (ResourceType resourceType2 in GetDerivedTypes(resourceType1))
                    {
                        yield return resourceType2;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the ResourceAssociationSet instance when given the source association end.
        /// </summary>
        /// <param name="resourceSet">Resource set of the source association end.</param>
        /// <param name="resourceType">Resource type of the source association end.</param>
        /// <param name="resourceProperty">Resource property of the source association end.</param>
        /// <returns>
        /// ResourceAssociationSet instance.
        /// </returns>
        public ResourceAssociationSet GetResourceAssociationSet(ResourceSet resourceSet, ResourceType resourceType, ResourceProperty resourceProperty)
        {
            if (resourceSet == null)
            {
                throw new ArgumentNullException("resourceSet");
            }

            if (resourceType == null)
            {
                throw new ArgumentNullException("resourceType");
            }

            if (resourceProperty == null)
            {
                throw new ArgumentNullException("resourceProperty");
            }

            ResourceSet resourceSet1;

            if (!TryResolveResourceSet(resourceSet.Name, out resourceSet1) || resourceSet1 != resourceSet)
            {
                throw new InvalidOperationException(string.Format(UnknownResourceSet, resourceSet.Name));
            }

            ResourceType resourceType1;

            if (!TryResolveResourceType(resourceType.FullName, out resourceType1) || resourceType1 != resourceType)
            {
                throw new InvalidOperationException(string.Format(UnknownResourceType, resourceType.FullName));
            }

            if (resourceType != resourceType.GetDeclaringTypeForProperty(resourceProperty))
            {
                throw new InvalidOperationException(string.Format(ResourceTypeMustBeDeclaringTypeForProperty, resourceType.FullName, resourceProperty.Name));
            }

            ResourceType resourceType2 = resourceProperty.ResourceType;

            if (resourceType2.ResourceTypeKind != ResourceTypeKind.EntityType)
            {
                throw new InvalidOperationException(string.Format(PropertyMustBeNavigationPropertyOnType, resourceProperty.Name, resourceType.FullName));
            }

            ResourceSet containerForResourceType = InternalGetContainerForResourceType(resourceType2.InstanceType, ResourceSets);
            return new ResourceAssociationSet(
                MetadataCacheItem.ResourceTypeCacheItems
                    .Count(rt => rt.ResourceType.Name == resourceType.Name) <= 1 ? resourceType.Name + '_' + resourceProperty.Name : resourceType.FullName.Replace('.', '_') + '_' + resourceProperty.Name,
                new ResourceAssociationSetEnd(resourceSet, resourceType, resourceProperty),
                new ResourceAssociationSetEnd(containerForResourceType, resourceType2, null));
        }

        /// <summary>
        /// Returns true if <paramref name="resourceType"/> represents an Entity Type which has derived Entity Types, else false.
        /// </summary>
        /// <param name="resourceType">instance of the resource type in question.</param>
        /// <returns>
        /// True if <paramref name="resourceType"/> represents an Entity Type which has derived Entity Types, else false.
        /// </returns>
        public bool HasDerivedTypes(ResourceType resourceType)
        {
            if (resourceType == null)
            {
                throw new ArgumentNullException("resourceType");
            }

            if (!ChildTypesCache.ContainsKey(resourceType))
            {
                throw new InvalidOperationException(string.Format(UnknownResourceTypeInstance, resourceType.FullName));
            }

            return ChildTypesCache[resourceType] != null;
        }

        /// <summary>
        /// Gets all available containers.
        /// </summary>
        /// <returns>
        /// An enumerable object with all available containers.
        /// </returns>
        public IEnumerable<ResourceSet> ResourceSets
        {
            get { return _metadata.EntitySets.Values; }
        }

        /// <summary>
        /// Returns all known service operations.
        /// </summary>
        public IEnumerable<ServiceOperation> ServiceOperations
        {
            get { return _metadata.ServiceOperations.Values; }
        }

        /// <summary>
        /// Given the specified name, tries to find a resource set.
        /// </summary>
        /// <param name="name">Name of the resource set to resolve.</param>
        /// <param name="resourceSet">Returns the resolved resource set, null if no resource set for the given name was found.</param>
        /// <returns>
        /// True if resource set with the given name was found, false otherwise.
        /// </returns>
        public bool TryResolveResourceSet(string name, out ResourceSet resourceSet)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            return _metadata.EntitySets.TryGetValue(name, out resourceSet);
        }

        /// <summary>
        /// Given the specified name, tries to find a type.
        /// </summary>
        /// <param name="name">Name of the type to resolve.</param>
        /// <param name="resourceType">Returns the resolved resource type, null if no resource type for the given name was found.</param>
        /// <returns>
        /// True if we found the resource type for the given name, false otherwise.
        /// </returns>
        public bool TryResolveResourceType(string name, out ResourceType resourceType)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            foreach (ResourceTypeCacheItem resourceTypeCacheItem in _metadata.ResourceTypeCacheItems)
            {
                if (resourceTypeCacheItem.ResourceType.FullName == name)
                {
                    resourceType = resourceTypeCacheItem.ResourceType;
                    return true;
                }
            }

            resourceType = null;
            return false;
        }

        /// <summary>
        /// Given the specified name, tries to find a service operation.
        /// </summary>
        /// <param name="name">Name of the service operation to resolve.</param>
        /// <param name="serviceOperation">Returns the resolved service operation, null if no service operation was found for the given name.</param>
        /// <returns>
        /// True if we found the service operation for the given name, false otherwise.
        /// </returns>
        public bool TryResolveServiceOperation(string name, out ServiceOperation serviceOperation)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            return _metadata.ServiceOperations.TryGetValue(name, out serviceOperation);
        }

        /// <summary>
        /// Returns all the types in this data source
        /// </summary>
        public virtual IEnumerable<ResourceType> Types
        {
            get { return _metadata.ResourceTypeCacheItems.Select(c => c.ResourceType); }
        }

        public object CurrentDataSource
        {
            get { return _dataSourceInstance; }
            set { throw new NotSupportedException(); }
        }

        #endregion

        #region IDataServiceQueryProvider members

        /// <summary>
        /// Gets the value of the open property.
        /// </summary>
        /// <param name="target">instance of the resource type.</param><param name="propertyName">name of the property.</param>
        /// <returns>
        /// the value of the open property. Currently this is not supported for Reflection provider.
        /// </returns>
        public object GetOpenPropertyValue(object target, string propertyName)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Returns the collection of open properties name and value for the given resource instance.
        /// </summary>
        /// <param name="target">instance of the resource.</param>
        /// <returns>
        /// Returns the collection of open properties name and value for the given resource instance. Currently not supported for Reflection provider.
        /// </returns>
        public IEnumerable<KeyValuePair<string, object>> GetOpenPropertyValues(object target)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Get the value of the strongly typed property.
        /// </summary>
        /// <param name="target">instance of the type declaring the property.</param><param name="resourceProperty">resource property describing the property.</param>
        /// <returns>
        /// value for the property.
        /// </returns>
        public object GetPropertyValue(object target, ResourceProperty resourceProperty)
        {
            if (target == null)
            {
                throw new ArgumentNullException("target");
            }

            if (resourceProperty == null)
            {
                throw new ArgumentNullException("resourceProperty");
            }

            try
            {
                return GetResourcePropertyCacheItem(ResolveNonPrimitiveTypeCacheItem(target.GetType()), resourceProperty).PropertyInfo.GetGetMethod().Invoke(target, null);
            }
            catch (TargetInvocationException ex)
            {
                CommonUtil.HandleTargetInvocationException(ex);
                throw;
            }
        }

        /// <summary>
        /// Returns the IQueryable that represents the container.
        /// </summary>
        /// <param name="container">resource set representing the entity set.</param>
        /// <returns>
        /// An IQueryable that represents the container; null if there is
        ///             no container for the specified name.
        /// </returns>
        public IQueryable GetQueryRootForResourceSet(ResourceSet container)
        {
            if (container == null)
            {
                throw new ArgumentNullException("container");
            }

            return GetQueryRootDelegate(container)(CurrentDataSource);
        }

        /// <summary>Gets the <see cref="ResourceType"/> for the specified <paramref name="resource"/>.</summary>
        /// <param name="resource">Instance to extract a <see cref="ResourceType"/> from.</param>
        /// <returns>The <see cref="ResourceType"/> that describes this <paramref name="resource"/> in this provider.</returns>
        public ResourceType GetResourceType(object resource)
        {
            if (resource == null)
            {
                throw new ArgumentNullException("resource");
            }

            return GetNonPrimitiveType(resource.GetType());
        }

        /// <summary>
        /// Invoke the given service operation instance.
        /// </summary>
        /// <param name="serviceOperation">metadata for the service operation to invoke.</param>
        /// <param name="parameters">list of parameters to pass to the service operation.</param>
        /// <returns>returns the result by the service operation instance.</returns>
        public virtual object InvokeServiceOperation(ServiceOperation serviceOperation, object[] parameters)
        {
            if (serviceOperation == null)
            {
                throw new ArgumentNullException("serviceOperation");
            }

            // DMD PATCH
            var data = serviceOperation.CustomState as Tuple<Type, MethodInfo>;

            if (data == null)
            {
                throw new InvalidOperationException(string.Format("Service operation {0} hasn't been created by {1}", serviceOperation.Name, typeof(CustomServiceOperationProvider).FullName));
            }

            try
            {
                return data.Item2.Invoke(data.Item1.IsInstanceOfType(_dataServiceInstance) ? _dataServiceInstance : _serviceProvider.GetService(data.Item1),
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy,
                    null, parameters, CultureInfo.InvariantCulture);

                // END PATCH
            }
            catch (TargetInvocationException exception)
            {
                CommonUtil.HandleTargetInvocationException(exception);
                throw;
            }
        }

        /// <summary>
        /// Gets a value indicating whether null propagation is required in expression trees.
        /// </summary>
        public bool IsNullPropagationRequired
        {
            get { return false; }
        }

        #endregion

        #region IDataServiceProviderBehavior members

        /// <summary>
        /// Instance of provider behavior that defines the assumptions service should make
        ///             about the provider.
        /// </summary>
        public ProviderBehavior ProviderBehavior
        {
            get { return EntityFrameworkProviderBehavior; }
        }

        #endregion

        #region IServiceProvider members

        /// <summary>
        /// Gets the service object of the specified type.
        /// </summary>
        /// <param name="serviceType">An object that specifies the type of service object to get.</param>
        /// <returns>
        /// A service object of type serviceType.-or- null if there is no service object of type serviceType.
        /// </returns>
        public virtual object GetService(Type serviceType)
        {
            if (typeof(IDataServiceMetadataProvider) == serviceType || typeof(IDataServiceQueryProvider) == serviceType || typeof(IDataServiceProviderBehavior) == serviceType)
            {
                return this;
            }

            return null;
        }

        #endregion

        /// <summary>
        /// Cache of immediate derived types per type.
        /// </summary>
        private Dictionary<ResourceType, List<ResourceType>> ChildTypesCache
        {
            get
            {
                return _metadata.ChildTypesCache;
            }
        }

        /// <summary>
        /// Target type for the data provider
        /// </summary>
        private Type DataSourceType
        {
            get { return CurrentDataSource.GetType(); }
        }

        /// <summary>
        /// Gets the MetadataCacheItem containing all the cached metadata.
        /// </summary>
        protected ProviderMetadataCacheItem MetadataCacheItem
        {
            get { return _metadata; }
        }

        /// <summary>
        /// Gets the MIME type declared on the specified <paramref name="member"/>.
        /// </summary>
        /// <param name="member">Member to check.</param>
        /// <returns>
        /// The MIME type declared on the specified <paramref name="member"/>; null
        ///             if no attribute is declared.
        /// </returns>
        internal static MimeTypeAttribute GetMimeTypeAttribute(MemberInfo member)
        {
            return member.ReflectedType.GetCustomAttributes(typeof(MimeTypeAttribute), true).Cast<MimeTypeAttribute>().FirstOrDefault(o => o.MemberName == member.Name);
        }

        /// <summary>
        /// Returns the type of the IEnumerable if the type implements IEnumerable interface; null otherwise.
        /// </summary>
        /// <param name="type">type that needs to be checked</param>
        /// <returns>
        /// Element type if the type implements IEnumerable, else returns null
        /// </returns>
        internal static Type GetIEnumerableElement(Type type)
        {
            return GetGenericInterfaceElementType(type, IEnumerableTypeFilter);
        }

        /// <summary>
        /// Returns the type of the IQueryable if the type implements IQueryable interface
        /// </summary>
        /// <param name="type">clr type on which IQueryable check needs to be performed.</param>
        /// <returns>
        /// Element type if the property type implements IQueryable, else returns null
        /// </returns>
        internal static Type GetIQueryableElement(Type type)
        {
            return GetGenericInterfaceElementType(type, IQueryableTypeFilter);
        }

        /// <summary>
        /// Releases the current data source object as necessary.
        /// </summary>
        /// <param name="disposing">Whether this method is called from an explicit call to Dispose by
        ///             the consumer, rather than during finalization.
        ///             </param>
        protected virtual void Dispose(bool disposing)
        {
            var disposable = _dataSourceInstance as IDisposable;

            if (disposable != null)
            {
                disposable.Dispose();
            }

            _dataSourceInstance = null;
        }

        /// <summary>
        /// Walks through the list of ancestors and finds the root base type and collects metadata for the entire chain of ancestors
        /// </summary>
        /// <param name="type">type whose ancestors metadata needs to be populated</param>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <param name="unvisitedTypes">list of unvisited types</param>
        /// <param name="entityTypeCandidate">Whether <paramref name="type"/> is a candidate to be an entity type.</param>
        /// <returns>
        /// return true if this given type is a entity type, otherwise returns false
        /// </returns>
        protected static ResourceType BuildHierarchyForEntityType(Type type, ProviderMetadataCacheItem metadataCacheItem, Queue<ResourceType> unvisitedTypes, bool entityTypeCandidate)
        {
            if (!type.IsVisible)
            {
                return null;
            }

            if (CommonUtil.IsUnsupportedType(type))
            {
                throw new InvalidOperationException(string.Format("Type {0} is not supported", type.FullName));
            }

            var list = new List<Type>();
            Type type1 = type;
            ResourceType resourceType1;

            for (resourceType1 = null; type1 != null && !TryGetType(metadataCacheItem, type1, out resourceType1); type1 = type1.BaseType)
            {
                list.Add(type1);
            }

            if (resourceType1 == null)
            {
                if (!entityTypeCandidate)
                {
                    return null;
                }

                for (int index = list.Count - 1; index >= 0; --index)
                {
                    if (CommonUtil.IsUnsupportedType(list[index]))
                    {
                        throw new InvalidOperationException(string.Format("Ancestor type {1} is not supported for {0}", type.FullName, list[index].FullName));
                    }

                    if (DoesTypeHaveKeyProperties(list[index], entityTypeCandidate))
                    {
                        break;
                    }

                    list.RemoveAt(index);
                }
            }
            else
            {
                if (resourceType1.ResourceTypeKind != ResourceTypeKind.EntityType)
                {
                    return null;
                }

                if (list.Count == 0)
                {
                    return resourceType1;
                }
            }

            for (int index = list.Count - 1; index >= 0; --index)
            {
                ResourceType resourceType2 = CreateResourceType(list[index], ResourceTypeKind.EntityType, resourceType1, metadataCacheItem);
                unvisitedTypes.Enqueue(resourceType2);
                resourceType1 = resourceType2;
            }

            return resourceType1;
        }

        /// <summary>
        /// Given a resource type, builds the EntityPropertyMappingInfo for each EntityPropertyMappingAttribute on it
        /// </summary>
        /// <param name="currentResourceType">Resouce type for which EntityPropertyMappingAttribute discovery is happening</param>
        private static void BuildReflectionEpmInfo(ResourceType currentResourceType)
        {
            if (currentResourceType.ResourceTypeKind != ResourceTypeKind.EntityType)
            {
                return;
            }

            foreach (EntityPropertyMappingAttribute attribute in
                currentResourceType.InstanceType.GetCustomAttributes(typeof(EntityPropertyMappingAttribute), currentResourceType.BaseType == null))
            {
                currentResourceType.AddEntityPropertyMappingAttribute(attribute);
            }
        }

        /// <summary>
        /// Return the set of IL instructions for getting the IQueryable instance for the given ResourceSet.
        /// </summary>
        /// <param name="resourceSet">ResourceSet instance.</param>
        /// <returns>
        /// Func to invoke to get IQueryable for the given ResourceSet.
        /// </returns>
        private Func<object, IQueryable> BuildQueryRootDelegate(ResourceSet resourceSet)
        {
            MethodInfo getMethod = DataSourceType.GetProperty(resourceSet.Name, BindingFlags.Instance | BindingFlags.Public).GetGetMethod();
            var dynamicMethod = new DynamicMethod("queryable_reader", typeof(IQueryable), new[] { typeof(object) }, false);
            ILGenerator ilGenerator = dynamicMethod.GetILGenerator();
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Castclass, DataSourceType);
            ilGenerator.Emit(OpCodes.Call, getMethod);
            ilGenerator.Emit(OpCodes.Ret);
            return (Func<object, IQueryable>)dynamicMethod.CreateDelegate(typeof(Func<object, IQueryable>));
        }

        /// <summary>
        /// Populates the metadata for the properties of the given resource type
        /// </summary>
        /// <param name="parentResourceType">resource type whose properties metadata needs to be populated</param>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <param name="unvisitedTypes">list of unvisited type</param>
        private static void BuildTypeProperties(ResourceType parentResourceType, ProviderMetadataCacheItem metadataCacheItem, Queue<ResourceType> unvisitedTypes)
        {
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public;

            if (parentResourceType.BaseType != null)
            {
                bindingFlags |= BindingFlags.DeclaredOnly;
            }

            var eTagProperties = new HashSet<string>(LoadETagProperties(parentResourceType), StringComparer.Ordinal);
            var resourceKeyKind = (ResourceKeyKind)2147483647;
            PropertyInfo[] properties = parentResourceType.InstanceType.GetProperties(bindingFlags);

            if (!properties.Any() && parentResourceType.BaseType == null)
            {
                throw new NotSupportedException(string.Format(ResourceTypeHasNoPublicallyVisibleProperties, parentResourceType.FullName));
            }

            foreach (PropertyInfo property1 in properties
                .Where(pi => Attribute.GetCustomAttribute(pi, typeof(NotMappedAttribute), true) == null))
            {
                if (!property1.CanRead || property1.GetIndexParameters().Length != 0)
                {
                    throw new InvalidOperationException(string.Format(InvalidProperty, property1.Name, parentResourceType.FullName));
                }

                var kind = (ResourcePropertyKind)(-1);
                Type type = property1.PropertyType;
                bool flag = false;
                ResourceType resourceType;

                if (!TryGetType(metadataCacheItem, type, out resourceType))
                {
                    Type ienumerableElement = GetIEnumerableElement(property1.PropertyType);

                    if (ienumerableElement != null)
                    {
                        TryGetType(metadataCacheItem, ienumerableElement, out resourceType);
                        flag = true;
                        type = ienumerableElement;
                    }
                }

                if (resourceType != null)
                {
                    if (resourceType.ResourceTypeKind == ResourceTypeKind.Primitive)
                    {
                        if (flag)
                        {
                            kind = ResourcePropertyKind.Collection;
                        }
                        else
                        {
                            ResourceKeyKind keyKind;

                            if (parentResourceType.BaseType == null &&
                                parentResourceType.ResourceTypeKind == ResourceTypeKind.EntityType &&
                                IsPropertyKeyProperty(property1, out keyKind))
                            {
                                if (keyKind < resourceKeyKind)
                                {
                                    if (parentResourceType.KeyProperties.Count != 0)
                                    {
                                        Debug.Assert(RemoveKeyPropertiesMethod != null);
                                        RemoveKeyPropertiesMethod.Invoke(parentResourceType, null);
                                    }

                                    resourceKeyKind = keyKind;
                                    kind = ResourcePropertyKind.Primitive | ResourcePropertyKind.Key;
                                }
                                else
                                {
                                    kind = keyKind != resourceKeyKind
                                        ? ResourcePropertyKind.Primitive
                                        : ResourcePropertyKind.Primitive | ResourcePropertyKind.Key;
                                }
                            }
                            else
                            {
                                kind = ResourcePropertyKind.Primitive;
                            }
                        }
                    }
                    else if (resourceType.ResourceTypeKind == ResourceTypeKind.ComplexType)
                    {
                        kind = flag ? ResourcePropertyKind.Collection : ResourcePropertyKind.ComplexType;
                    }
                    else if (resourceType.ResourceTypeKind == ResourceTypeKind.EntityType)
                    {
                        kind = flag ? ResourcePropertyKind.ResourceSetReference : ResourcePropertyKind.ResourceReference;
                    }
                }
                else
                {
                    resourceType = IsEntityOrComplexType(type, metadataCacheItem, unvisitedTypes);

                    if (resourceType != null)
                    {
                        if (resourceType.ResourceTypeKind == ResourceTypeKind.ComplexType)
                        {
                            if (flag)
                            {
                                if (GetIEnumerableElement(type) != null)
                                {
                                    throw new InvalidOperationException(string.Format(CollectionOfCollectionProperty, property1.Name, parentResourceType.FullName));
                                }

                                kind = ResourcePropertyKind.Collection;
                            }
                            else
                            {
                                kind = ResourcePropertyKind.ComplexType;
                            }
                        }
                        else
                        {
                            kind = flag ? ResourcePropertyKind.ResourceSetReference : ResourcePropertyKind.ResourceReference;
                        }
                    }
                }
                if (resourceType == null || resourceType.ResourceTypeKind == ResourceTypeKind.EntityType && parentResourceType.ResourceTypeKind == ResourceTypeKind.ComplexType)
                {
                    if (resourceType != null)
                    {
                        throw new InvalidOperationException(string.Format(ComplexTypeWithNavigationProperty, property1.Name, parentResourceType.FullName));
                    }

                    if (flag && GetIEnumerableElement(type) != null)
                    {
                        throw new InvalidOperationException(string.Format(CollectionOfCollectionProperty, property1.Name, parentResourceType.FullName));
                    }

                    if (flag)
                    {
                        throw new InvalidOperationException(string.Format(CollectionOfUnsupportedTypeProperty, property1.Name, parentResourceType.FullName, type));
                    }

                    if (CommonUtil.IsUnsupportedType(type))
                    {
                        throw new InvalidOperationException(string.Format(UnsupportedPropertyType, property1.Name, parentResourceType.FullName));
                    }

                    throw new InvalidOperationException(string.Format(InvalidProperty, property1.Name, parentResourceType.FullName));
                }

                if (resourceType.ResourceTypeKind == ResourceTypeKind.EntityType && InternalGetContainerForResourceType(type, metadataCacheItem.EntitySets.Values) == null)
                {
                    throw new InvalidOperationException(string.Format(EntityPropertyWithNoEntitySet, parentResourceType.FullName, property1.Name));
                }

                if (kind == ResourcePropertyKind.Collection)
                {
                    resourceType = ResourceType.GetCollectionResourceType(resourceType);
                }

                if (eTagProperties.Remove(property1.Name))
                {
                    kind |= ResourcePropertyKind.ETag;
                }

                var property2 = new ResourceProperty(property1.Name, kind, resourceType);
                MimeTypeAttribute mimeTypeAttribute = GetMimeTypeAttribute(property1);

                if (mimeTypeAttribute != null)
                {
                    property2.MimeType = mimeTypeAttribute.MimeType;
                }

                parentResourceType.AddProperty(property2);
            }

            if (parentResourceType.ResourceTypeKind == ResourceTypeKind.EntityType && (parentResourceType.KeyProperties == null || parentResourceType.KeyProperties.Count == 0))
            {
                throw new InvalidOperationException(string.Format(KeyPropertiesCannotBeIgnored, parentResourceType.FullName));
            }

            if (eTagProperties.Count != 0)
            {
                throw new InvalidOperationException(string.Format(ETagPropertyNameNotValid, eTagProperties.First(), parentResourceType.FullName));
            }
        }

        /// <summary>
        /// Checks that the metadata model is consistent.
        /// </summary>
        protected virtual void CheckModelConsistency()
        {
            //
        }

        /// <summary>
        /// returns the new resource type instance
        /// </summary>
        /// <param name="type">backing clr type for the resource.</param>
        /// <param name="kind">kind of the resource.</param>
        /// <param name="baseType">base type of the resource.</param>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <returns>
        /// returns a new instance of the resource type containing all the metadata.
        /// </returns>
        private static ResourceType CreateResourceType(Type type, ResourceTypeKind kind, ResourceType baseType, ProviderMetadataCacheItem metadataCacheItem)
        {
            var resourceType = new ResourceType(type, kind, baseType, type.Namespace, CommonUtil.GetModelTypeName(type), type.IsAbstract);
            resourceType.IsOpenType = false;

            if (type.GetCustomAttributes(typeof(HasStreamAttribute), true).Length == 1)
            {
                resourceType.IsMediaLinkEntry = true;
            }

            foreach (object obj in type.GetCustomAttributes(typeof (NamedStreamAttribute), baseType == null))
            {
                resourceType.AddProperty(new ResourceProperty(((NamedStreamAttribute)obj).Name, ResourcePropertyKind.Stream, PrimitiveResourceTypeMap.TypeMap.GetPrimitive(typeof(Stream))));
            }

            metadataCacheItem.AddResourceType(type, resourceType);
            Dictionary<ResourceType, List<ResourceType>> childTypesCache = metadataCacheItem.ChildTypesCache;
            childTypesCache.Add(resourceType, null);

            if (baseType != null)
            {
                if (childTypesCache[baseType] == null)
                {
                    childTypesCache[baseType] = new List<ResourceType>();
                }

                childTypesCache[baseType].Add(resourceType);
            }

            return resourceType;
        }

        /// <summary>
        /// Checks whether there is a key defined for the given type.
        /// </summary>
        /// <param name="type">type to check </param>
        /// <param name="entityTypeCandidate">
        /// Whether <paramref name="type"/> is being considered as a possible 
        /// entity type.
        /// </param>
        /// <returns>returns true if there are one or key properties present else returns false</returns>
        private static bool DoesTypeHaveKeyProperties(Type type, bool entityTypeCandidate)
        {
            Debug.Assert(type != null, "type != null");

            // Check for properties declared on this element only
            foreach (PropertyInfo property in type.GetProperties(CommonUtil.PublicInstanceBindingFlags | BindingFlags.DeclaredOnly))
            {
                ResourceKeyKind keyKind;

                if (IsPropertyKeyProperty(property, out keyKind))
                {
                    if (keyKind == ResourceKeyKind.AttributedKey && !entityTypeCandidate)
                    {
                        throw new InvalidOperationException(string.Format(UnsupportedType, type.FullName));
                    }

                    if (!entityTypeCandidate)
                    {
                        return false;
                    }

                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Called by the service to let the provider perform data model validation.
        /// </summary>
        /// <param name="knownTypes">Collection of known types.</param>
        /// <param name="useMetadataCacheOrder">Whether to use metadata cache ordering instead of default ordering.</param>
        private void FinalizeMetadataModel(IEnumerable<Type> knownTypes, bool useMetadataCacheOrder)
        {
            Debug.Assert(knownTypes != null, "knownTypes != null");

            if (_metadataRequiresInitialization)
            {
                PopulateMetadataForUserSpecifiedTypes(knownTypes, _metadata);

                if (useMetadataCacheOrder)
                {
                    foreach (ResourceSet resourceSet in _metadata.EntitySets.Values)
                    {
                        resourceSet.UseMetadataKeyOrder = true;
                    }
                }

                CheckModelConsistency();

                MakeMetadataReadonly();
            }
        }

        /// <summary>
        /// Returns the "T" in the IQueryable of T implementation of type.
        /// </summary>
        /// <param name="type">Type to check.</param>
        /// <param name="typeFilter">filter against which the type is checked</param>
        /// <returns>
        /// The element type for the generic IQueryable interface of the type,
        ///             or null if it has none or if it's ambiguous.
        /// </returns>
        private static Type GetGenericInterfaceElementType(Type type, TypeFilter typeFilter)
        {
            if (typeFilter(type, null))
            {
                return type.GetGenericArguments()[0];
            }

            Type[] interfaces = type.FindInterfaces(typeFilter, null);

            if (interfaces.Length != 1)
            {
                return null;
            }

            return interfaces[0].GetGenericArguments()[0];
        }

        /// <summary>
        /// Returns the resource type for the corresponding clr type.
        /// If the given clr type is a collection, then resource type describes the element type of the collection.
        /// </summary>
        /// <param name="type">clrType whose corresponding resource type needs to be returned</param>
        /// <returns>Returns the resource type</returns>
        protected ResourceType GetNonPrimitiveType(Type type)
        {
            Debug.Assert(type != null, "type != null");

            // Check for the type directly first
            ResourceType resourceType = ResolveNonPrimitiveType(type);

            if (resourceType == null)
            {
                // check for ienumerable types
                Type elementType = GetIEnumerableElement(type);

                if (elementType != null)
                {
                    resourceType = PrimitiveResourceTypeMap.TypeMap.GetPrimitive(elementType) ?? ResolveNonPrimitiveType(elementType);
                }
            }

            return resourceType;
        }

        /// <summary>
        /// Get the QueryRoot delegate for the given ResourceSet.
        /// </summary>
        /// <param name="resourceSet">ResourceSet instance.</param>
        /// <returns>the delegate for the given ResourceSet.</returns>
        private Func<object, IQueryable> GetQueryRootDelegate(ResourceSet resourceSet)
        {
            Debug.Assert(resourceSet != null, "resourceSet != null");
            Func<object, IQueryable> queryRootDelegate;
            _metadata.QueryRootCache.TryGetValue(resourceSet, out queryRootDelegate);
            Debug.Assert(queryRootDelegate != null, "queryRootDelegate != null");
            return queryRootDelegate;
        }

        /// <summary>
        /// Get the PropertyInfo for the given resource property
        /// </summary>
        /// <param name="resourceTypeCacheItem">Instance of ResourceTypeCacheItem containing the ResourceType instance.</param><param name="resourceProperty">ResourceProperty instance.</param>
        /// <returns>
        /// PropertyInfo instance for the given ResourceProperty.
        /// </returns>
        protected ResourcePropertyCacheItem GetResourcePropertyCacheItem(ResourceTypeCacheItem resourceTypeCacheItem, ResourceProperty resourceProperty)
        {
            ResourceType declaringTypeForProperty = resourceTypeCacheItem.ResourceType.GetDeclaringTypeForProperty(resourceProperty);

            if (declaringTypeForProperty != resourceTypeCacheItem.ResourceType)
            {
                resourceTypeCacheItem = ResolveNonPrimitiveTypeCacheItem(declaringTypeForProperty.InstanceType);
            }

            return resourceTypeCacheItem.GetResourcePropertyCacheItem(resourceTypeCacheItem.ResourceType.InstanceType, resourceProperty);
        }

        /// <summary>
        /// Checks whether the given type is a generic type with a generic parameter.
        /// </summary>
        /// <param name="type">type which needs to be checked.</param>
        /// <returns>
        /// Returns true, if the <paramref name="type"/> is generic and has generic parameters. Otherwise returns false.
        /// </returns>
        private static bool HasGenericParameters(Type type)
        {
            return type.IsGenericType && type.GetGenericArguments().Any(type1 => type1.IsGenericParameter);
        }

        /// <summary>
        /// Filter callback for finding IEnumerable implementations.
        /// </summary>
        /// <param name="m">Type to inspect.</param>
        /// <param name="filterCriteria">Filter criteria.</param>
        /// <returns>
        /// true if the specified type is an IEnumerable of T; false otherwise.
        /// </returns>
        private static bool IEnumerableTypeFilter(Type m, object filterCriteria)
        {
            return m.IsGenericType && m.GetGenericTypeDefinition() == typeof(IEnumerable<>);
        }

        /// <summary>
        /// Get the resource set for the given clr type.
        /// </summary>
        /// <param name="type">clr type for which resource set name needs to be returned</param>
        /// <param name="entitySets">Available entity sets to consider.</param>
        /// <returns>
        /// The container for its type, null if not found.
        /// </returns>
        private static ResourceSet InternalGetContainerForResourceType(Type type, IEnumerable<ResourceSet> entitySets)
        {
            return entitySets.FirstOrDefault(resourceSet => resourceSet.ResourceType.InstanceType.IsAssignableFrom(type));
        }

        /// <summary>
        /// Checks whether the specified type is a complex type.
        /// </summary>
        /// <param name="type">Type to check.</param>
        /// <returns>
        /// true if the specified type is a complex type; false otherwise. Note
        ///             that resources are not distinguished from complex types.
        /// </returns>
        private static bool IsComplexType(Type type)
        {
            return type.IsVisible && !type.IsArray && (!type.IsPointer && !type.IsCOMObject)
                && (!type.IsInterface && !(type == typeof(IntPtr)) && (!(type == typeof(UIntPtr)) && !(type == typeof(char)))) && (!(type == typeof(TimeSpan))
                && !(type == typeof(DateTimeOffset)) && (!(type == typeof(Uri)) && !type.IsEnum));
        }

        /// <summary>
        /// If the given type is a entity or complex type, it returns the resource type corresponding to the given type
        /// 
        /// </summary>
        /// <param name="type">clr type</param>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <param name="unvisitedTypes">list of unvisited types</param>
        /// <returns>
        /// resource type corresponding to the given clr type, if the clr type is entity or complex
        /// </returns>
        private static ResourceType IsEntityOrComplexType(Type type, ProviderMetadataCacheItem metadataCacheItem, Queue<ResourceType> unvisitedTypes)
        {
            if (type.IsValueType || CommonUtil.IsUnsupportedType(type))
            {
                return null;
            }

            ResourceType resourceType = BuildHierarchyForEntityType(type, metadataCacheItem, unvisitedTypes, false);

            if (resourceType == null && IsComplexType(type))
            {
                resourceType = CreateResourceType(type, ResourceTypeKind.ComplexType, null, metadataCacheItem);
                unvisitedTypes.Enqueue(resourceType);
            }

            return resourceType;
        }

        /// <summary>
        /// Checks whether the given property is a key property.
        /// </summary>
        /// <param name="property">property to check</param>
        /// <param name="keyKind">returns the key kind of the property, based on the heuristic it matches</param>
        /// <returns>
        /// true if this is a key property, else returns false
        /// </returns>
        private static bool IsPropertyKeyProperty(PropertyInfo property, out ResourceKeyKind keyKind)
        {
            keyKind = (ResourceKeyKind)(-1);

            if (CommonUtil.IsPrimitiveType(property.PropertyType) && !property.PropertyType.IsGenericType)
            {
                DataServiceKeyAttribute serviceKeyAttribute = property.ReflectedType.GetCustomAttributes(true).OfType<DataServiceKeyAttribute>().FirstOrDefault();

                if (serviceKeyAttribute != null && serviceKeyAttribute.KeyNames.Contains(property.Name))
                {
                    keyKind = ResourceKeyKind.AttributedKey;
                    return true;
                }

                if (property.Name.Equals(property.DeclaringType.Name + "ID", StringComparison.InvariantCultureIgnoreCase))
                {
                    keyKind = ResourceKeyKind.TypeNameId;
                    return true;
                }

                if (property.Name.Equals("ID", StringComparison.InvariantCultureIgnoreCase))
                {
                    keyKind = ResourceKeyKind.Id;
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Filter callback for finding IQueryable implementations.
        /// </summary>
        /// <param name="m">Type to inspect.</param>
        /// <param name="filterCriteria">Filter criteria.</param>
        /// <returns>
        /// true if the specified type is an IQueryable of T; false otherwise.
        /// </returns>
        private static bool IQueryableTypeFilter(Type m, object filterCriteria)
        {
            if (m.IsGenericType)
            {
                return m.GetGenericTypeDefinition() == typeof(IQueryable<>);
            }

            return false;
        }

        /// <summary>
        /// Looks up the metadata in the cache. If not present in the cache, then loads metadata from the provider.
        /// </summary>
        /// <param name="skipServiceOperations">Should service operations be loaded.</param>
        protected void LoadMetadata(bool skipServiceOperations)
        {
            Type type1 = _dataServiceInstance.GetType();
            Type type2 = _dataSourceInstance.GetType();

            _metadata = MetadataCache<ProviderMetadataCacheItem>.TryLookup(type1);

            if (_metadata != null)
            {
                return;
            }

            _metadata = new ProviderMetadataCacheItem(type2);
            PopulateMetadata(_metadata);

            if (!skipServiceOperations)
            {
                LoadServiceOperations();
            }

            _metadataRequiresInitialization = true;

            FinalizeMetadataModel(Enumerable.Empty<Type>(), false);
        }

        /// <summary>
        /// Loads the etag properties for the given resource type
        /// 
        /// </summary>
        /// <param name="resourceType">resource type whose etag property names need to be loaded.</param>
        /// <returns>
        /// the list of properties that form the etag for the given resource type.
        /// </returns>
        private static IEnumerable<string> LoadETagProperties(ResourceType resourceType)
        {
            bool inherit = resourceType.BaseType == null;
            var etagAttributeArray = (ETagAttribute[])resourceType.InstanceType.GetCustomAttributes(typeof(ETagAttribute), inherit);

            if (etagAttributeArray.Length == 1)
            {
                return etagAttributeArray[0].PropertyNames;
            }

            return Enumerable.Empty<string>();
        }

        /// <summary>
        /// Adds service operations based on methods on the data service type.
        /// </summary>
        private void LoadServiceOperations()
        {
            Type type = _dataServiceInstance.GetType();

            foreach (ServiceOperation serviceOperation in new CustomServiceOperationProvider(type, ResolveResourceType, ResolveResourceSet).ServiceOperations)
            {
                if (_metadata.ServiceOperations.ContainsKey(serviceOperation.Name))
                {
                    throw new InvalidOperationException(string.Format(OverloadingNotSupported, type, serviceOperation.CustomState));
                }

                _metadata.ServiceOperations.Add(serviceOperation.Name, serviceOperation);
            }

            // DMD PATCH
            foreach (Type iface in _serviceInterfaces)
            {
                foreach (ServiceOperation serviceOperation in new CustomServiceOperationProvider(iface, ResolveResourceType, ResolveResourceSet).ServiceOperations)
                {
                    if (_metadata.ServiceOperations.ContainsKey(serviceOperation.Name))
                    {
                        throw new InvalidOperationException(string.Format(OverloadingNotSupported, type, serviceOperation.CustomState));
                    }

                    _metadata.ServiceOperations.Add(serviceOperation.Name, serviceOperation);
                }
            }
            // END PATCH
        }

        /// <summary>
        /// Make all the metadata readonly
        /// </summary>
        private void MakeMetadataReadonly()
        {
            Debug.Assert(_metadataRequiresInitialization, "Should only call when initializing metadata.");

            foreach (ResourceSet container in ResourceSets)
            {
                container.SetReadOnly();
            }

            foreach (ResourceType resourceType in Types)
            {
                resourceType.SetReadOnly();

                // This will cause EPM information to be initialized
                resourceType.PropertiesDeclaredOnThisType.Count();
            }

            foreach (ServiceOperation operation in this.ServiceOperations)
            {
                operation.SetReadOnly();
            }

            // After metadata has been completely loaded, add it to the cache.
            _metadata = MetadataCache<ProviderMetadataCacheItem>.AddCacheItem(_dataServiceInstance.GetType(), _metadata);
        }

        /// <summary>
        /// Populates the metadata for this provider.
        /// </summary>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem in which metadata needs to be populated.</param>
        protected virtual void PopulateMetadata(ProviderMetadataCacheItem metadataCacheItem)
        {
            var unvisitedTypes = new Queue<ResourceType>();

            foreach (PropertyInfo propertyInfo in DataSourceType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(pi => pi.CanRead && pi.GetIndexParameters().Length == 0)
                .Where(pi => Attribute.GetCustomAttribute(pi, typeof(NotMappedAttribute), true) == null))
            {
                Type iqueryableElement = GetIQueryableElement(propertyInfo.PropertyType);

                if (iqueryableElement != null)
                {
                    ResourceType elementType = BuildHierarchyForEntityType(iqueryableElement, metadataCacheItem, unvisitedTypes, true);

                    if (elementType == null)
                    {
                        throw new InvalidOperationException(string.Format(InvalidEntitySetProperty, propertyInfo.Name, XmlConvert.EncodeName(ContainerName)));
                    }

                    foreach (KeyValuePair<string, ResourceSet> keyValuePair in metadataCacheItem.EntitySets)
                    {
                        Type instanceType = keyValuePair.Value.ResourceType.InstanceType;

                        if (instanceType.IsAssignableFrom(iqueryableElement))
                        {
                            throw new InvalidOperationException(string.Format(MultipleEntitySetsForSameType, keyValuePair.Value.Name, propertyInfo.Name, instanceType.FullName, elementType.FullName));
                        }

                        if (iqueryableElement.IsAssignableFrom(instanceType))
                        {
                            throw new InvalidOperationException(string.Format(MultipleEntitySetsForSameType, propertyInfo.Name, keyValuePair.Value.Name, elementType.FullName, instanceType.FullName));
                        }
                    }

                    var resourceSet = new ResourceSet(propertyInfo.Name, elementType);
                    metadataCacheItem.EntitySets.Add(propertyInfo.Name, resourceSet);
                    metadataCacheItem.QueryRootCache.Add(resourceSet, BuildQueryRootDelegate(resourceSet));
                }
            }

            PopulateMetadataForTypes(metadataCacheItem, unvisitedTypes);
            PopulateMetadataForDerivedTypes(metadataCacheItem, unvisitedTypes);
        }

        /// <summary>
        /// Find out all the derived types in the list of assemblies and then populate metadata for those types
        /// </summary>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <param name="unvisitedTypes">list of unvisited types</param>
        protected static void PopulateMetadataForDerivedTypes(ProviderMetadataCacheItem metadataCacheItem, Queue<ResourceType> unvisitedTypes)
        {
            List<ResourceType> resourceTypes = metadataCacheItem.EntitySets.Values.Select(resourceSet => resourceSet.ResourceType).ToList();
            var processedAssemblies = new HashSet<Assembly>(EqualityComparer<Assembly>.Default);
            var types = new List<Type>();

            foreach (ResourceTypeCacheItem resourceTypeCacheItem in metadataCacheItem.ResourceTypeCacheItems)
            {
                ResourceType resourceType = resourceTypeCacheItem.ResourceType;

                if (resourceType.ResourceTypeKind != ResourceTypeKind.Primitive)
                {
                    Assembly assembly = resourceType.InstanceType.Assembly;

                    if (!processedAssemblies.Contains(assembly))
                    {
                        foreach (Type type in assembly.GetTypes())
                        {
                            if (type.IsVisible && !HasGenericParameters(type) && metadataCacheItem.TryGetResourceType(type) == null)
                            {
                                types.AddRange(resourceTypes.Where(t => t.InstanceType.IsAssignableFrom(type)).Select(t => type));
                            }
                        }

                        processedAssemblies.Add(assembly);
                    }
                }
            }

            foreach (Type type in types)
            {
                BuildHierarchyForEntityType(type, metadataCacheItem, unvisitedTypes, false);
                PopulateMetadataForTypes(metadataCacheItem, unvisitedTypes);
            }
        }

        /// <summary>
        /// Populate metadata for the given clr type.
        /// </summary>
        /// <param name="type">type whose metadata needs to be loaded.</param>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <returns>
        /// resource type containing metadata for the given clr type.
        /// </returns>
        private ResourceType PopulateMetadataForType(Type type, ProviderMetadataCacheItem metadataCacheItem)
        {
            var unvisitedTypes = new Queue<ResourceType>();
            ResourceType resourceType;

            if (!TryGetType(metadataCacheItem, type, out resourceType))
            {
                resourceType = IsEntityOrComplexType(type, metadataCacheItem, unvisitedTypes);

                if (resourceType != null)
                {
                    PopulateMetadataForTypes(metadataCacheItem, unvisitedTypes);
                }
            }

            return resourceType;
        }

        /// <summary>
        /// Populate types for metadata specified by the provider
        /// </summary>
        /// <param name="userSpecifiedTypes">list of types specified by the provider</param>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        private void PopulateMetadataForUserSpecifiedTypes(IEnumerable<Type> userSpecifiedTypes, ProviderMetadataCacheItem metadataCacheItem)
        {
            var unvisitedTypes = new Queue<ResourceType>();

            foreach (Type type in userSpecifiedTypes)
            {
                ResourceType resourceType;

                if (!TryGetType(metadataCacheItem, type, out resourceType) && IsEntityOrComplexType(type, metadataCacheItem, unvisitedTypes) == null)
                {
                    throw new InvalidOperationException(string.Format(InvalidTypeSpecified, type.FullName));
                }
            }

            PopulateMetadataForTypes(metadataCacheItem, unvisitedTypes);
        }

        /// <summary>
        /// Populates the metadata for the given unvisited types and all the associated types with this type
        /// </summary>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <param name="unvisitedTypes">list of unvisited type</param>
        protected static void PopulateMetadataForTypes(ProviderMetadataCacheItem metadataCacheItem, Queue<ResourceType> unvisitedTypes)
        {
            while (unvisitedTypes.Count != 0)
            {
                ResourceType resourceType = unvisitedTypes.Dequeue();
                BuildTypeProperties(resourceType, metadataCacheItem, unvisitedTypes);
                BuildReflectionEpmInfo(resourceType);
            }
        }

        /// <summary>
        /// Returns the resource type for the corresponding clr type.
        /// </summary>
        /// <param name="type">clrType whose corresponding resource type needs to be returned</param>
        /// <returns>Returns the resource type</returns>
        protected ResourceType ResolveNonPrimitiveType(Type type)
        {
            var metadataCacheItem = ResolveNonPrimitiveTypeCacheItem(type);
            return metadataCacheItem != null ? metadataCacheItem.ResourceType : null;
        }

        /// <summary>
        /// Returns the resource type for the corresponding clr type.
        /// </summary>
        /// <param name="type">clrType whose corresponding resource type needs to be returned</param>
        /// <returns>
        /// Returns the resource type
        /// </returns>
        protected ResourceTypeCacheItem ResolveNonPrimitiveTypeCacheItem(Type type)
        {
            return _metadata.TryGetResourceTypeCacheItem(type);
        }

        /// <summary>
        /// Given a <see cref="T:System.Data.Services.Providers.ResourceType"/>, finds the corresponding <see cref="T:System.Data.Services.Providers.ResourceSet"/>.
        /// </summary>
        /// <param name="resourceType">Given resource type.</param>
        /// <param name="method">Method implementing service operation.</param>
        /// <returns>
        /// <see cref="T:System.Data.Services.Providers.ResourceSet"/> corresponding to <paramref name="resourceType"/>.
        /// </returns>
        private ResourceSet ResolveResourceSet(ResourceType resourceType, MethodInfo method)
        {
            ResourceSet container;
            TryFindAnyContainerForType(resourceType, out container);
            return container;
        }

        /// <summary>
        /// Given a CLR type, provides the corresponding <see cref="T:System.Data.Services.Providers.ResourceType"/> by either looking it up, or loading it's metadata.
        /// </summary>
        /// <param name="type">CLR type for which resource type is being looked up.</param>
        /// <returns>
        /// <see cref="T:System.Data.Services.Providers.ResourceType"/> corresponding to <paramref name="type"/>.
        /// </returns>
        private ResourceType ResolveResourceType(Type type)
        {
            return PopulateMetadataForType(type, _metadata);
        }

        /// <summary>
        /// Looks for the first resource set that the specified <paramref name="type"/> could belong to.
        /// </summary>
        /// <param name="type">Type to look for.</param>
        /// <param name="container">After the method returns, the container to which the type could belong.</param>
        /// <returns>
        /// true if a container was found; false otherwise.
        /// </returns>
        private bool TryFindAnyContainerForType(ResourceType type, out ResourceSet container)
        {
            foreach (ResourceSet resourceSet in _metadata.EntitySets.Values)
            {
                Debug.Assert(IsAssignableFromMethod != null);

                if ((bool)IsAssignableFromMethod.Invoke(resourceSet.ResourceType, new object[] { type }))
                {
                    container = resourceSet;
                    return true;
                }
            }

            container = null;
            return false;
        }

        /// <summary>
        /// Find the corresponding ResourceType for a given Type, primitive or not
        /// </summary>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem.</param>
        /// <param name="type">Type to look for</param>
        /// <param name="resourceType">Corresponding ResourceType, if found</param>
        /// <returns>
        /// True if type found, false otherwise
        /// </returns>
        private static bool TryGetType(ProviderMetadataCacheItem metadataCacheItem, Type type, out ResourceType resourceType)
        {
            resourceType = PrimitiveResourceTypeMap.TypeMap.GetPrimitive(type) ?? metadataCacheItem.TryGetResourceType(type);
            return resourceType != null;
        }
    }
}
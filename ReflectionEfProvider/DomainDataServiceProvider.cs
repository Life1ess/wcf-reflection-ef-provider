using System;
using System.Collections.Generic;
using System.Data.Services;
using System.Data.Services.Providers;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace ReflectionEfProvider
{
    public abstract class DomainDataServiceProvider : ReflectionEntityFrameworkDataServiceProvider, IDataServiceUpdateProvider2, IDataServiceEntityFrameworkProvider
    {
        private const string ComplexTypeExpected = "A complex resource type is expected, however the resource type '{0}' is of type kind '{1}'.";
        private const string DerivedEntityTypes = "Entity type '{0}' is an ancestor for type '{1}'. Please make sure that there is only one IQueryable property or known entity type for each type hierarchy.";
        private const string EntityTypeExpected = "An entity resource type is expected, however the resource type '{0}' is of type kind '{1}'.";
        private const string ErrorInSettingPropertyValue = "Error processing request stream. Error encountered in setting value for property '{0}'. Please verify that the value is correct.";
        private const string NotEntityType = "Entity type {0} cannot be used as such.";        
        private const string PropertyValueCannotBeSet = "Error processing request stream. Property '{0}' is a read-only property and cannot be updated. Please make sure that this property is not present in the request payload.";
        private const string PropertyNotDefinedOnType = "The resource type '{0}' does not define a property that is named '{1}'.";
        private const string TargetElementTypeOfTheUriSpecifiedDoesNotMatchWithTheExpectedType = "The entity type '{0}' that the URI refers to does not match with the expected entity type '{1}'.";
        private const string UnknownResourceTypeForClrType = "The clr type '{0}' is an unknown resource type to the metadata provider.";

        private readonly ICollection<Type> _entityTypes;

        /// <summary>
        /// List of service actions to be invoked during SaveChanges().
        /// </summary>
        private readonly List<IDataServiceInvokable> _actionsToInvoke = new List<IDataServiceInvokable>();

        #region IDataServiceUpdateProvider2 members

        /// <summary>
        /// Initializes a new DomainDataServiceProvider instance.
        /// </summary>
        /// <param name="dataServiceInstance">data service instance.</param>
        /// <param name="dataSourceInstance">data source instance.</param>
        /// <param name="serviceLocator">service locator</param>
        /// <param name="serviceInterfaces">list of services to query for service operations</param>
        /// <param name="entityTypes">custom entity types to add to the model</param>
        protected DomainDataServiceProvider(object dataServiceInstance, object dataSourceInstance, IServiceProvider serviceLocator, ICollection<Type> serviceInterfaces = null, ICollection<Type> entityTypes = null)
            : base(dataServiceInstance, dataSourceInstance, serviceLocator, serviceInterfaces)
        {
            _entityTypes = entityTypes;
        }

        /// <summary>
        /// Creates the resource of the given type and belonging to the given container
        /// </summary>
        /// <param name="containerName">container name to which the resource needs to be added</param>
        /// <param name="fullTypeName">full type name i.e. Namespace qualified type name of the resource</param>
        /// <returns>object representing a resource of given type and belonging to the given container</returns>
        public object CreateResource(string containerName, string fullTypeName)
        {
            if (string.IsNullOrEmpty(fullTypeName))
            {
                throw new ArgumentNullException("fullTypeName");
            }

            ResourceType resourceType;

            if (!TryResolveResourceType(fullTypeName, out resourceType))
            {
                throw new InvalidOperationException(string.Format("Unable to find resource type {0}", fullTypeName));
            }

            Debug.Assert(resourceType != null, "resourceType != null");

            object resource;

            if (containerName != null)
            {
                if (resourceType.ResourceTypeKind != ResourceTypeKind.EntityType)
                {
                    throw new InvalidOperationException(string.Format(EntityTypeExpected, resourceType.FullName, resourceType.ResourceTypeKind));
                }

                IWriteObjectService writeService = GetWriteObjectService(resourceType);
                resource = Activator.CreateInstance(resourceType.InstanceType);
                writeService.Create(resource);
            }
            else
            {
                // When the container name is null, it means we are trying to create a instance of complex types.
                if (resourceType.ResourceTypeKind != ResourceTypeKind.ComplexType)
                {
                    throw new InvalidOperationException(string.Format(ComplexTypeExpected, resourceType.FullName, resourceType.ResourceTypeKind));
                }

                resource = GetConstructorDelegate(resourceType)();
            }

            return resource;
        }

        /// <summary>
        /// Gets the resource of the given type that the query points to
        /// </summary>
        /// <param name="query">query pointing to a particular resource</param>
        /// <param name="fullTypeName">full type name i.e. Namespace qualified type name of the resource</param>
        /// <returns>object representing a resource of given type and as referenced by the query</returns>
        public object GetResource(IQueryable query, string fullTypeName)
        {
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            object result = null;

            foreach (object resource in query)
            {
                if (result != null)
                {
                    throw new InvalidOperationException("Single resource expected");
                }

                result = resource;
            }

            if (result != null)
            {
                ResourceType resourceType = GetResourceType(result);

                if (resourceType == null)
                {
                    throw new InvalidOperationException(string.Format(UnknownResourceTypeForClrType, result.GetType().FullName));
                }

                if (fullTypeName != null && resourceType.FullName != fullTypeName)
                {
                    throw CreateBadRequestError(string.Format(TargetElementTypeOfTheUriSpecifiedDoesNotMatchWithTheExpectedType, resourceType.FullName, fullTypeName));
                }
            }

            return result;
        }

        /// <summary>
        /// Resets the value of the given resource to its default value
        /// </summary>
        /// <param name="resource">resource whose value needs to be reset</param>
        /// <returns>same resource with its value reset</returns>
        public object ResetResource(object resource)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sets the value of the given property on the target object
        /// </summary>
        /// <param name="targetResource">target object which defines the property</param>
        /// <param name="propertyName">name of the property whose value needs to be updated</param>
        /// <param name="propertyValue">value of the property</param>
        public void SetValue(object targetResource, string propertyName, object propertyValue)
        {
            if (targetResource == null)
            {
                throw new ArgumentNullException("targetResource");
            }

            if (string.IsNullOrEmpty(propertyName))
            {
                throw new ArgumentNullException("propertyName");
            }

            ResourceTypeCacheItem resourceTypeCacheItem = ResolveNonPrimitiveTypeCacheItem(targetResource.GetType());

            if (resourceTypeCacheItem == null)
            {
                throw new InvalidOperationException(string.Format(UnknownResourceTypeForClrType, targetResource.GetType().FullName));
            }

            ResourceType resourceType = resourceTypeCacheItem.ResourceType;
            ResourceProperty resourceProperty = resourceType.Properties.FirstOrDefault(p => p.Name == propertyName && (p.Kind & ResourcePropertyKind.Stream) == 0);

            if (resourceProperty == null)
            {
                throw new InvalidOperationException(string.Format(PropertyNotDefinedOnType, resourceType.FullName, propertyName));
            }

            MethodInfo setMethod = GetResourcePropertyCacheItem(resourceTypeCacheItem, resourceProperty).PropertyInfo.GetSetMethod();

            if (setMethod == null)
            {
                throw CreateBadRequestError(string.Format(PropertyValueCannotBeSet, resourceProperty.Name));
            }

            try
            {
                setMethod.Invoke(targetResource, new[] { propertyValue });
            }
            catch (TargetInvocationException exception)
            {
                CommonUtil.HandleTargetInvocationException(exception);
                throw;
            }
            catch (ArgumentException exception)
            {
                throw CreateBadRequestError(string.Format(ErrorInSettingPropertyValue, resourceProperty.Name), exception);
            }
        }

        /// <summary>
        /// Gets the value of the given property on the target object
        /// </summary>
        /// <param name="targetResource">target object which defines the property</param>
        /// <param name="propertyName">name of the property whose value needs to be updated</param>
        /// <returns>the value of the property for the given target resource</returns>
        public object GetValue(object targetResource, string propertyName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sets the value of the given reference property on the target object
        /// </summary>
        /// <param name="targetResource">target object which defines the property</param>
        /// <param name="propertyName">name of the property whose value needs to be updated</param>
        /// <param name="propertyValue">value of the property</param>
        public void SetReference(object targetResource, string propertyName, object propertyValue)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Adds the given value to the collection
        /// </summary>
        /// <param name="targetResource">target object which defines the property</param>
        /// <param name="propertyName">name of the property whose value needs to be updated</param>
        /// <param name="resourceToBeAdded">value of the property which needs to be added</param>
        public void AddReferenceToCollection(object targetResource, string propertyName, object resourceToBeAdded)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Removes the given value from the collection
        /// </summary>
        /// <param name="targetResource">target object which defines the property</param>
        /// <param name="propertyName">name of the property whose value needs to be updated</param>
        /// <param name="resourceToBeRemoved">value of the property which needs to be removed</param>
        public void RemoveReferenceFromCollection(object targetResource, string propertyName, object resourceToBeRemoved)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Delete the given resource
        /// </summary>
        /// <param name="resource">resource that needs to be deleted</param>
        public void DeleteResource(object resource)
        {
            if (resource == null)
            {
                throw new ArgumentNullException("resource");
            }

            ResourceType resourceType = GetResourceType(resource);

            if (resourceType == null)
            {
                throw new InvalidOperationException(string.Format(UnknownResourceTypeForClrType, resource.GetType().FullName));
            }

            GetWriteObjectService(resourceType).Delete(resource);
        }

        /// <summary>
        /// Saves all the pending changes made till now
        /// </summary>
        public void SaveChanges()
        {
            foreach (IDataServiceInvokable invokable in _actionsToInvoke)
            {
                invokable.Invoke();
            }

            GetUnitOfWork().Flush();
        }

        /// <summary>
        /// Returns the actual instance of the resource represented by the given resource object
        /// </summary>
        /// <param name="resource">object representing the resource whose instance needs to be fetched</param>
        /// <returns>The actual instance of the resource represented by the given resource object</returns>
        public object ResolveResource(object resource)
        {
            return resource;
        }

        /// <summary>
        /// Revert all the pending changes.
        /// </summary>
        public void ClearChanges()
        {
            GetUnitOfWork().Rollback();
        }

        /// <summary>
        /// Set the etag values for the given resource.
        /// </summary>
        /// <param name="resource">resource for which etag values need to be set.</param>
        /// <param name="checkForEquality">true if we need to compare the property values for equality. If false, then we need to compare values for non-equality.</param>
        /// <param name="concurrencyValues">list of the etag property names, along with their values.</param>
        public void SetConcurrencyValues(object resource, bool? checkForEquality, IEnumerable<KeyValuePair<string, object>> concurrencyValues)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Queues up the <paramref name="invokable"/> to be invoked during IUpdatable.SaveChanges().
        /// </summary>
        /// <param name="invokable">The invokable instance whose Invoke() method will be called during IUpdatable.SaveChanges().</param>
        public void ScheduleInvokable(IDataServiceInvokable invokable)
        {
            if (invokable == null)
            {
                throw new ArgumentNullException();
            }

            _actionsToInvoke.Add(invokable);
        }

        #endregion

        #region IDataServiceEntityFrameworkProvider members

        /// <summary>
        /// Get the list of etag property names given the entity set name and the instance of the resource
        /// </summary>
        /// <param name="containerName">name of the entity set</param>
        /// <param name="resourceType">Type of the resource whose etag properties need to be fetched</param>
        /// <returns>list of etag property names</returns>
        public IList<ResourceProperty> GetETagProperties(string containerName, ResourceType resourceType)
        {
            return new ResourceProperty[0];
        }

        /// <summary>
        /// Return the schema version for the EF provider.
        /// </summary>
        public MetadataEdmSchemaVersion EdmSchemaVersion
        {
            get { return MetadataEdmSchemaVersion.Version3Dot0; }
        }

        #endregion

        /// <summary>
        /// Invoke the given service operation instance.
        /// </summary>
        /// <param name="serviceOperation">metadata for the service operation to invoke.</param>
        /// <param name="parameters">list of parameters to pass to the service operation.</param>
        /// <returns>returns the result by the service operation instance.</returns>
        public override object InvokeServiceOperation(ServiceOperation serviceOperation, object[] parameters)
        {
            var data = serviceOperation.CustomState as Tuple<Type, MethodInfo>;

            if (data == null)
            {
                throw new InvalidOperationException(string.Format("Service operation {0} hasn't been created by {1}", serviceOperation.Name, typeof(CustomServiceOperationProvider).FullName));
            }

            ParameterInfo[] parameterInfos = data.Item2.GetParameters();

            for (int i = 0; i < parameterInfos.Length; ++i)
            {
                if (parameters[i] != null)
                {
                    ResourceType resourceType = GetNonPrimitiveType(parameterInfos[i].ParameterType);

                    if (resourceType != null && resourceType.ResourceTypeKind == ResourceTypeKind.EntityType)
                    {
                        object entity = GetReadObjectService(resourceType).GetById(parameters[i]);

                        if (entity == null)
                        {
                            throw new InvalidOperationException(string.Format("Entity resolver could not resolve entity of type {0} with key {1}", resourceType.FullName, parameters[i]));
                        }

                        parameters[i] = entity;
                    }
                }
            }

            object result = base.InvokeServiceOperation(serviceOperation, parameters);

            if (result != null)
            {
                Type queryType = result.GetType().GetInterface(typeof(IQueryable<>).Name);

                if (queryType != null)
                {
                    Type elementType = queryType.GenericTypeArguments.Single();
                    ResourceType resourceType = GetNonPrimitiveType(elementType);

                    if (resourceType == null)
                    {
                        throw new InvalidOperationException("Service operation must return query of entity types");
                    }

                    GetReadObjectService(resourceType).ProxyCreationEnabled = false;
                }
            }

            return result;
        }

        /// <summary>
        /// Populates the metadata for this provider.
        /// </summary>
        /// <param name="metadataCacheItem">Instance of ProviderMetadataCacheItem in which metadata needs to be populated.</param>
        protected override void PopulateMetadata(ProviderMetadataCacheItem metadataCacheItem)
        {
            base.PopulateMetadata(metadataCacheItem);

            var unvisitedTypes = new Queue<ResourceType>();

            foreach (Type entityType in _entityTypes)
            {
                ResourceType elementType = BuildHierarchyForEntityType(entityType, metadataCacheItem, unvisitedTypes, true);

                if (elementType == null)
                {
                    throw new InvalidOperationException(string.Format(NotEntityType, entityType.FullName));
                }

                foreach (KeyValuePair<string, ResourceSet> keyValuePair in metadataCacheItem.EntitySets)
                {
                    Type instanceType = keyValuePair.Value.ResourceType.InstanceType;

                    if (instanceType.IsAssignableFrom(entityType))
                    {
                        throw new InvalidOperationException(string.Format(DerivedEntityTypes, keyValuePair.Value.Name, elementType.Name));
                    }

                    if (entityType.IsAssignableFrom(instanceType))
                    {
                        throw new InvalidOperationException(string.Format(DerivedEntityTypes, elementType.Name, keyValuePair.Value.Name));
                    }
                }

                Func<object, IQueryable> queryRootDelegate;

                try
                {
                    GetReadObjectService(elementType);
                    queryRootDelegate = _ => GetReadObjectService(elementType).GetAll();
                }
                catch (InvalidOperationException)
                {
                    queryRootDelegate = _ => Array.CreateInstance(elementType.InstanceType, 0).AsQueryable();
                }

                var resourceSet = new ResourceSet(elementType.Name, elementType);
                metadataCacheItem.EntitySets.Add(elementType.Name, resourceSet);
                metadataCacheItem.QueryRootCache.Add(resourceSet, queryRootDelegate);
            }

            PopulateMetadataForTypes(metadataCacheItem, unvisitedTypes);
            PopulateMetadataForDerivedTypes(metadataCacheItem, unvisitedTypes);
        }

        protected abstract IReadObjectService GetReadObjectService(ResourceType resourceType);

        protected abstract IWriteObjectService GetWriteObjectService(ResourceType resourceType);

        protected abstract IUnitOfWork GetUnitOfWork();

        /// <summary>
        /// Creates a new exception to indicate BadRequest error.
        /// </summary>
        /// <param name="message">Plain text error message for this exception.</param>
        /// <param name="innerException">Inner Exception.</param>
        /// <returns>A new exception to indicate a bad request error.</returns>
        private static DataServiceException CreateBadRequestError(string message, Exception innerException = null)
        {
            // 400 - Bad Request
            return new DataServiceException(400, null, message, null, innerException);
        }

        /// <summary>
        /// Gets the constructor delegate for the given ResourceType from the cache.
        /// </summary>
        /// <param name="resourceType">ResourceType instance.</param>
        /// <returns>the constructor delegate for the given ResourceType from the cache.</returns>
        private Func<object> GetConstructorDelegate(ResourceType resourceType)
        {
            var cacheEntry = MetadataCacheItem.TryGetResourceTypeCacheItem(resourceType.InstanceType);
            return cacheEntry.ConstructorDelegate;
        }
    }
}
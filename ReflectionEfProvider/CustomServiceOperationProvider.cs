using System;
using System.Collections.Generic;
using System.Data.Services;
using System.Data.Services.Providers;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.ServiceModel.Web;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Provides the service writers capability to specify the type which implements
    /// the service operations.
    /// </summary>
    internal class CustomServiceOperationProvider
    {
        private const string IEnumerableAlwaysMultiple = "Type '{0}' has a method '{1}' which is a generic IEnumerable but is marked with a SingleResultAttribute. Only IQueryable methods support this attribute.";
        private const string ParameterNotIn = "Method '{0}' has a parameter '{1}' which is not an [in] parameter.";
        private const string ParameterTypeNotSupported = "Method '{0}' has a parameter '{1}' of type '{2}' which is not supported for service operations. Only primitive types are supported as parameters.";
        private const string EntityTypeNotSupported = "Method '{0}' has an entity parameter '{1}' which key type '{2}' is not supported for service operations. Only single primitive type is supported as entity key type.";
        private const string EnumTypeNotSupported = "Method '{0}' has an enum parameter '{1}' which underlying type '{2}' is not supported for service operations. Only primitive types are supported as enum underlying types.";
        private const string ServiceOperationMissingSingleEntitySet = "Service operation '{0}' produces instances of type '{1}', but having a single entity set for that type is required.";
        private const string TypeIsAbstract = "Unable to create CustomServiceOperationProvider. Type '{0}' is abstract.";
        private const string UnsupportedReturnType = "Unable to load metadata for return type '{1}' of method '{0}'.";

        /// <summary>
        /// Type implementing service operations.
        /// </summary>
        private readonly Type _type;

        /// <summary>
        /// Resolver that gives a <see cref="ResourceType"/> corresponding to given CLR type.
        /// </summary>
        private readonly Func<Type, ResourceType> _resourceTypeResolver;

        /// <summary>
        /// Resolver that gives a <see cref="ResourceSet"/> corresponding to given <see cref="ResourceType"/> and <see cref="MethodInfo"/>.
        /// </summary>
        private readonly Func<ResourceType, MethodInfo, ResourceSet> _resourceSetResolver;

        /// <summary>
        /// Lazily one-time initialized collection of service operations.
        /// </summary>
        private readonly Lazy<List<ServiceOperation>> _serviceOperations;

        /// <summary>
        /// Constructs a new instance of CustomServiceOperationProvider.
        /// </summary>
        /// <param name="type">Type implementing service operations.</param>
        /// <param name="resourceTypeResolver">Resolver that gives a <see cref="ResourceSet"/> corresponding to given <see cref="ResourceType"/> and <see cref="MethodInfo"/>.</param>
        /// <param name="resourceSetResolver">Resolver that gives a <see cref="ResourceType"/> corresponding to given CLR type.</param>
        public CustomServiceOperationProvider(Type type, Func<Type, ResourceType> resourceTypeResolver, Func<ResourceType, MethodInfo, ResourceSet> resourceSetResolver)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            if (resourceTypeResolver == null)
            {
                throw new ArgumentNullException("resourceTypeResolver");
            }

            if (resourceSetResolver == null)
            {
                throw new ArgumentNullException("resourceSetResolver");
            }

            if (type.IsAbstract && !type.IsInterface)
            {
                throw new InvalidOperationException(string.Format(TypeIsAbstract, type));
            }

            _type = type;
            _resourceTypeResolver = resourceTypeResolver;
            _resourceSetResolver = resourceSetResolver;
            _serviceOperations = new Lazy<List<ServiceOperation>>(FindServiceOperations, true);
        }

        /// <summary>
        /// Returns all the service operations exposed on the registered types.
        /// </summary>
        /// <returns>Collection of service operations.</returns>
        public IEnumerable<ServiceOperation> ServiceOperations
        {
            get { return _serviceOperations.Value; }
        }

        public static string GetServiceOperationName(Type service, MethodInfo method)
        {
            string serviceName = service.Name;

            if (serviceName.StartsWith("I"))
            {
                serviceName = serviceName.Substring(1);
            }

            return serviceName + "_" + method.Name;
        }

        /// <summary>
        /// Iterates over all the interesting methods on the type passed in the constructor and infers
        /// all the service operations from it.
        /// </summary>
        /// <returns>A list of service operations inferred from the type provided in the constructor.</returns>
        private List<ServiceOperation> FindServiceOperations()
        {
            var serviceOps = new List<ServiceOperation>();

            foreach (MethodInfo methodInfo in _type.GetMethods(CommonUtil.PublicInstanceBindingFlags | BindingFlags.FlattenHierarchy))
            {
                if (methodInfo.GetCustomAttributes(typeof(WebGetAttribute), true).Length != 0)
                {
                    serviceOps.Add(GetServiceOperationForMethod(_type, methodInfo, CommonUtil.HttpMethodGet));
                }
                else if (methodInfo.GetCustomAttributes(typeof(WebInvokeAttribute), true).Length != 0)
                {
                    serviceOps.Add(GetServiceOperationForMethod(_type, methodInfo, CommonUtil.HttpMethodPost));
                }
            }

            return serviceOps;
        }

        /// <summary>
        /// Returns a new <see cref="ServiceOperation"/> based on the specified <paramref name="method"/>
        /// instance.
        /// </summary>
        /// <param name="service">Service that declares operation</param>
        /// <param name="method">Method to expose as a service operation.</param>
        /// <param name="protocolMethod">Protocol (for example HTTP) method the service operation responds to.</param>
        /// <returns>Service operation corresponding to give <paramref name="method"/>.</returns>
        private ServiceOperation GetServiceOperationForMethod(Type service, MethodInfo method, string protocolMethod)
        {
            Debug.Assert(method != null, "method != null");
            Debug.Assert(!method.IsAbstract || service.IsInterface, "!method.IsAbstract - if method is abstract, the type is abstract - already checked");

            bool hasSingleResult = MethodHasSingleResult(method);
            ServiceOperationResultKind resultKind;
            ResourceType resourceType = null;

            if (method.ReturnType == typeof(void))
            {
                resultKind = ServiceOperationResultKind.Void;
            }
            else
            {
                // Load the metadata of the resource type on the fly.
                // For Edm provider, it might not mean anything, but for reflection service provider, we need to
                // load the metadata of the type if its used only in service operation case
                Type resultType;

                if (CommonUtil.IsPrimitiveType(method.ReturnType))
                {
                    resultKind = ServiceOperationResultKind.DirectValue;
                    resultType = method.ReturnType;
                    resourceType = PrimitiveResourceTypeMap.TypeMap.GetPrimitive(resultType);
                }
                else
                {
                    Type queryableElement = ReflectionEntityFrameworkDataServiceProvider.GetIQueryableElement(method.ReturnType);

                    if (queryableElement != null)
                    {
                        resultKind = hasSingleResult ? ServiceOperationResultKind.QueryWithSingleResult : ServiceOperationResultKind.QueryWithMultipleResults;
                        resultType = queryableElement;
                    }
                    else
                    {
                        Type enumerableElement = ReflectionEntityFrameworkDataServiceProvider.GetIEnumerableElement(method.ReturnType);

                        if (enumerableElement != null)
                        {
                            resultKind = ServiceOperationResultKind.Enumeration;
                            resultType = enumerableElement;
                        }
                        else
                        {
                            resultType = method.ReturnType;
                            resultKind = ServiceOperationResultKind.DirectValue;
                        }
                    }

                    Debug.Assert(resultType != null, "resultType != null");
                    resourceType = PrimitiveResourceTypeMap.TypeMap.GetPrimitive(resultType) ?? ResolveResourceType(resultType);
                }

                if (resourceType == null)
                {
                    throw new InvalidOperationException(string.Format(UnsupportedReturnType, method, method.ReturnType));
                }

                if (resultKind == ServiceOperationResultKind.Enumeration && hasSingleResult)
                {
                    throw new InvalidOperationException(string.Format(IEnumerableAlwaysMultiple, _type, method));
                }
            }

            ParameterInfo[] parametersInfo = method.GetParameters();
            var parameters = new ServiceOperationParameter[parametersInfo.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                ParameterInfo parameterInfo = parametersInfo[i];
                
                if (parameterInfo.IsOut || parameterInfo.IsRetval)
                {
                    throw new InvalidOperationException(string.Format(ParameterNotIn, method, parameterInfo));
                }

                ResourceType parameterType = PrimitiveResourceTypeMap.TypeMap.GetPrimitive(parameterInfo.ParameterType);

                // DMD PATCH
                string parameterName = parameterInfo.Name ?? "p" + i.ToString(CultureInfo.InvariantCulture);

                if (parameterType == null && parameterInfo.ParameterType.IsEnum)
                {
                    Type underlyingType = parameterInfo.ParameterType.GetEnumUnderlyingType();
                    parameterType = PrimitiveResourceTypeMap.TypeMap.GetPrimitive(underlyingType);

                    if (parameterType == null)
                    {
                        throw new InvalidOperationException(string.Format(EnumTypeNotSupported, method, parameterInfo, underlyingType));                        
                    }
                }

                if (parameterType == null)
                {
                    ResourceType entityResourceType = ResolveResourceType(parameterInfo.ParameterType);

                    if (entityResourceType != null && entityResourceType.ResourceTypeKind == ResourceTypeKind.EntityType)
                    {
                        if (entityResourceType.KeyProperties.Count != 1)
                        {
                            throw new InvalidOperationException(string.Format(EntityTypeNotSupported, method, parameterInfo.Name, entityResourceType.FullName));
                        }

                        parameterType = entityResourceType.KeyProperties.Single().ResourceType;
                        parameterName += "Id";
                    }
                }

                if (parameterType == null)
                {
                    throw new InvalidOperationException(string.Format(ParameterTypeNotSupported, method, parameterInfo, parameterInfo.ParameterType));
                }

                parameters[i] = new ServiceOperationParameter(parameterName, parameterType);
                // END DMD PATCH
            }

            ResourceSet resourceSet = null;

            if (resourceType != null && resourceType.ResourceTypeKind == ResourceTypeKind.EntityType)
            {
                resourceSet = ResolveResourceSet(resourceType, method);

                if (resourceSet == null)
                {
                    throw new InvalidOperationException(string.Format(ServiceOperationMissingSingleEntitySet, method, resourceType.FullName));
                }
            }

            var operation = new ServiceOperation(GetServiceOperationName(service, method), resultKind, resourceType, resourceSet, protocolMethod, parameters);
            operation.CustomState = Tuple.Create(service, method);
            MimeTypeAttribute attribute = ReflectionEntityFrameworkDataServiceProvider.GetMimeTypeAttribute(method);

            if (attribute != null)
            {
                operation.MimeType = attribute.MimeType;
            }

            return operation;
        }

        /// <summary>Checks whether the specified method has a SingleResultAttribute declared on it.</summary>
        /// <param name="method">Method to check.</param>
        /// <returns>
        /// true if the specified method (in its declared type or in an 
        /// ancestor declaring the type) has the SingleResultAttribute set.
        /// </returns>
        private static bool MethodHasSingleResult(MethodInfo method)
        {
            Debug.Assert(method != null, "method != null");
            return method.GetCustomAttributes(typeof(SingleResultAttribute), true).Length > 0;
        }

        /// <summary>
        /// Method for obtaining a <see cref="ResourceType"/> corresponding to the given CLR type.
        /// </summary>
        /// <param name="type">CLR type.</param>
        /// <returns><see cref="ResourceType"/> correspoding to <paramref name="type"/>.</returns>
        private ResourceType ResolveResourceType(Type type)
        {
            Debug.Assert(_resourceTypeResolver != null, "ResourceType resolver must be initialized.");
            return _resourceTypeResolver(type);
        }

        /// <summary>
        /// Method for obtaining a <see cref="ResourceSet"/> corresponding to given resource type.
        /// </summary>
        /// <param name="resourceType">Given resource type.</param>
        /// <param name="methodInfo">MethodInfo for a service operation.</param>
        /// <returns><see cref="ResourceSet"/> corresponding to <paramref name="resourceType"/>.</returns>
        private ResourceSet ResolveResourceSet(ResourceType resourceType, MethodInfo methodInfo)
        {
            Debug.Assert(_resourceSetResolver != null, "ResourceSet resolver must be initialized.");
            return _resourceSetResolver(resourceType, methodInfo);
        }
    }
}
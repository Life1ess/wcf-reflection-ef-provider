using System;
using System.Data.Services;
using System.Data.Services.Providers;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace ReflectionEfProvider
{
    internal static class CommonUtil
    {
        /// <summary>
        /// List of types unsupported by the client
        /// </summary>
        private static readonly Type[] UnsupportedTypes =
        {
            typeof (IDynamicMetaObjectProvider),
            typeof (Tuple<>),
            typeof (Tuple<,>),
            typeof (Tuple<,,>),
            typeof (Tuple<,,,>),
            typeof (Tuple<,,,,>),
            typeof (Tuple<,,,,,>),
            typeof (Tuple<,,,,,,>),
            typeof (Tuple<,,,,,,,>)
        };

        /// <summary>
        /// HTTP method name for GET requests.
        /// </summary>
        internal const string HttpMethodGet = "GET";

        /// <summary>
        /// HTTP method name for POST requests.
        /// </summary>
        internal const string HttpMethodPost = "POST";

        /// <summary>
        /// Creates a delegate that when called creates a new instance of the specified <paramref name="type"/>.
        /// </summary>
        /// <param name="type">Type of the instance.</param><param name="fullName">full name of the given clr type.
        ///             If the type name is not specified, it takes the full name from the clr type.</param><param name="targetType">Type to return from the delegate.</param>
        /// <returns>
        /// A delegate that when called creates a new instance of the specified <paramref name="type"/>.
        /// </returns>
        internal static Delegate CreateNewInstanceConstructor(Type type, string fullName, Type targetType)
        {
            ConstructorInfo constructor = type.GetConstructor(Type.EmptyTypes);

            if (constructor == null)
            {
                fullName = fullName ?? type.FullName;
                throw new InvalidOperationException(string.Format("No empty constructor found for type {0}", fullName));
            }

            var dynamicMethod = new DynamicMethod("invoke_constructor", targetType, Type.EmptyTypes, false);
            ILGenerator ilGenerator = dynamicMethod.GetILGenerator();
            ilGenerator.Emit(OpCodes.Newobj, constructor);

            if (targetType.IsValueType)
            {
                ilGenerator.Emit(OpCodes.Box);
            }

            ilGenerator.Emit(OpCodes.Ret);
            return dynamicMethod.CreateDelegate(typeof(Func<>).MakeGenericType(new[] { targetType }));
        }

        /// <summary>
        /// Gets the resource type which the resource property is declared on.
        /// </summary>
        /// <param name="resourceType">resource type to start looking</param><param name="resourceProperty">resource property in question</param><param name="rootType">root type in the hierarchy at which we need to stop.</param>
        /// <returns>
        /// actual resource type that declares the property or the root type if the property is declared in a more base type than the given root type.
        /// </returns>
        internal static ResourceType GetDeclaringTypeForProperty(this ResourceType resourceType, ResourceProperty resourceProperty, ResourceType rootType = null)
        {
            while (resourceType != rootType && !resourceType.PropertiesDeclaredOnThisType.Contains(resourceProperty))
                resourceType = resourceType.BaseType;
            return resourceType;
        }

        /// <summary>
        /// Gets the type name (without namespace) of the specified <paramref name="type"/>,
        ///             appropriate as an externally-visible type name.
        /// </summary>
        /// <param name="type">Type to get name for.</param>
        /// <returns>
        /// The type name for <paramref name="type"/>.
        /// </returns>
        internal static string GetModelTypeName(Type type)
        {
            if (type.IsGenericType)
            {
                Type[] genericArguments = type.GenericTypeArguments;
                var stringBuilder = new StringBuilder(type.Name.Length * 2 * (1 + genericArguments.Length));

                if (type.IsNested)
                {
                    stringBuilder.Append(GetModelTypeName(type.DeclaringType));
                    stringBuilder.Append('_');
                }

                stringBuilder.Append(type.Name);
                stringBuilder.Append('[');

                for (int index = 0; index < genericArguments.Length; ++index)
                {
                    if (index > 0)
                    {
                        stringBuilder.Append(' ');
                    }

                    if (genericArguments[index].IsGenericParameter)
                    {
                        stringBuilder.Append(genericArguments[index].Name);
                    }
                    else
                    {
                        string modelTypeNamespace = GetModelTypeNamespace(genericArguments[index]);

                        if (!string.IsNullOrEmpty(modelTypeNamespace))
                        {
                            stringBuilder.Append(modelTypeNamespace);
                            stringBuilder.Append('.');
                        }

                        stringBuilder.Append(GetModelTypeName(genericArguments[index]));
                    }
                }

                stringBuilder.Append(']');
                return stringBuilder.ToString();
            }

            if (type.IsNested)
            {
                return GetModelTypeName(type.DeclaringType) + "_" + type.Name;
            }

            return type.Name;
        }

        /// <summary>
        /// Gets the type namespace of the specified <paramref name="type"/>,
        ///             appropriate as an externally-visible type name.
        /// </summary>
        /// <param name="type">Type to get namespace for.</param>
        /// <returns>
        /// The namespace for <paramref name="type"/>.
        /// </returns>
        internal static string GetModelTypeNamespace(Type type)
        {
            return type.Namespace ?? string.Empty;
        }

        /// <summary>Handles the specified <paramref name='exception'/>.</summary>
        /// <param name='exception'>Exception to handle</param>
        /// <remarks>The caller should re-throw the original exception if this method returns normally.</remarks>
        internal static void HandleTargetInvocationException(TargetInvocationException exception)
        {
            Debug.Assert(exception != null, "exception != null");

            var dataException = exception.InnerException as DataServiceException;

            if (dataException == null)
            {
                return;
            }

            throw new DataServiceException(
                dataException.StatusCode,
                dataException.ErrorCode,
                dataException.Message,
                dataException.MessageLanguage,
                exception);
        }

        /// <summary>
        /// Checks whether the specified type is a known primitive type.
        /// </summary>
        /// <param name="type">Type to check.</param>
        /// <returns>
        /// true if the specified type is known to be a primitive type; false otherwise.
        /// </returns>
        internal static bool IsPrimitiveType(Type type)
        {
            if (type == null)
            {
                return false;
            }

            return PrimitiveResourceTypeMap.TypeMap.IsPrimitive(type);
        }

        /// <summary>
        /// Test whether a type is unsupported by the client lib
        /// </summary>
        /// <param name="type">The type to test</param>
        /// <returns>
        /// Returns true if the type is not supported
        /// </returns>
        internal static bool IsUnsupportedType(Type type)
        {
            if (type.IsGenericType)
            {
                type = type.GetGenericTypeDefinition();
            }

            return UnsupportedTypes.Any(t => t.IsAssignableFrom(type));
        }

        /// <summary>
        /// Bindings Flags for public instance members.
        /// </summary>
        internal const BindingFlags PublicInstanceBindingFlags = BindingFlags.Instance | BindingFlags.Public;
    }
}
using System;
using System.Collections.Generic;
using System.Data.Services.Providers;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Spatial;
using System.Threading;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Mapping between primitive CLR types, EDM type names, and ResourceTypes
    /// </summary>
    internal class PrimitiveResourceTypeMap
    {
        /// <summary>
        /// List of primitive types supported by WCF Data Services and their corresponding EDM names.
        /// </summary>
        private static readonly KeyValuePair<Type, string>[] BuiltInTypesMapping =
        {
            new KeyValuePair<Type, string>(typeof (string), "Edm.String"),
            new KeyValuePair<Type, string>(typeof (bool), "Edm.Boolean"),
            new KeyValuePair<Type, string>(typeof (bool?), "Edm.Boolean"),
            new KeyValuePair<Type, string>(typeof (byte), "Edm.Byte"),
            new KeyValuePair<Type, string>(typeof (byte?), "Edm.Byte"),
            new KeyValuePair<Type, string>(typeof (DateTime), "Edm.DateTime"),
            new KeyValuePair<Type, string>(typeof (DateTime?), "Edm.DateTime"),
            new KeyValuePair<Type, string>(typeof (Decimal), "Edm.Decimal"),
            new KeyValuePair<Type, string>(typeof (Decimal?), "Edm.Decimal"),
            new KeyValuePair<Type, string>(typeof (double), "Edm.Double"),
            new KeyValuePair<Type, string>(typeof (double?), "Edm.Double"),
            new KeyValuePair<Type, string>(typeof (Guid), "Edm.Guid"),
            new KeyValuePair<Type, string>(typeof (Guid?), "Edm.Guid"),
            new KeyValuePair<Type, string>(typeof (short), "Edm.Int16"),
            new KeyValuePair<Type, string>(typeof (short?), "Edm.Int16"),
            new KeyValuePair<Type, string>(typeof (int), "Edm.Int32"),
            new KeyValuePair<Type, string>(typeof (int?), "Edm.Int32"),
            new KeyValuePair<Type, string>(typeof (long), "Edm.Int64"),
            new KeyValuePair<Type, string>(typeof (long?), "Edm.Int64"),
            new KeyValuePair<Type, string>(typeof (sbyte), "Edm.SByte"),
            new KeyValuePair<Type, string>(typeof (sbyte?), "Edm.SByte"),
            new KeyValuePair<Type, string>(typeof (float), "Edm.Single"),
            new KeyValuePair<Type, string>(typeof (float?), "Edm.Single"),
            new KeyValuePair<Type, string>(typeof (byte[]), "Edm.Binary"),
            new KeyValuePair<Type, string>(typeof (Stream), "Edm.Stream"),
            new KeyValuePair<Type, string>(typeof (Geography), "Edm.Geography"),
            new KeyValuePair<Type, string>(typeof (GeographyPoint), "Edm.GeographyPoint"),
            new KeyValuePair<Type, string>(typeof (GeographyLineString), "Edm.GeographyLineString"),
            new KeyValuePair<Type, string>(typeof (GeographyPolygon), "Edm.GeographyPolygon"),
            new KeyValuePair<Type, string>(typeof (GeographyCollection), "Edm.GeographyCollection"),
            new KeyValuePair<Type, string>(typeof (GeographyMultiLineString), "Edm.GeographyMultiLineString"),
            new KeyValuePair<Type, string>(typeof (GeographyMultiPoint), "Edm.GeographyMultiPoint"),
            new KeyValuePair<Type, string>(typeof (GeographyMultiPolygon), "Edm.GeographyMultiPolygon"),
            new KeyValuePair<Type, string>(typeof (Geometry), "Edm.Geometry"),
            new KeyValuePair<Type, string>(typeof (GeometryPoint), "Edm.GeometryPoint"),
            new KeyValuePair<Type, string>(typeof (GeometryLineString), "Edm.GeometryLineString"),
            new KeyValuePair<Type, string>(typeof (GeometryPolygon), "Edm.GeometryPolygon"),
            new KeyValuePair<Type, string>(typeof (GeometryCollection), "Edm.GeometryCollection"),
            new KeyValuePair<Type, string>(typeof (GeometryMultiLineString), "Edm.GeometryMultiLineString"),
            new KeyValuePair<Type, string>(typeof (GeometryMultiPoint), "Edm.GeometryMultiPoint"),
            new KeyValuePair<Type, string>(typeof (GeometryMultiPolygon), "Edm.GeometryMultiPolygon"),
            new KeyValuePair<Type, string>(typeof (TimeSpan), "Edm.Time"),
            new KeyValuePair<Type, string>(typeof (TimeSpan?), "Edm.Time"),
            new KeyValuePair<Type, string>(typeof (DateTimeOffset), "Edm.DateTimeOffset"),
            new KeyValuePair<Type, string>(typeof (DateTimeOffset?), "Edm.DateTimeOffset"),
            //new KeyValuePair<Type, string>(typeof (XElement), "Edm.String"),
            //new KeyValuePair<Type, string>(typeof (Binary), "Edm.Binary")
        };

        /// <summary>
        /// List of primitive type that can be derived from.
        /// </summary>
        private static readonly Type[] InheritablePrimitiveClrTypes =
        {
            typeof (Geography),
            typeof (GeographyPoint),
            typeof (GeographyLineString),
            typeof (GeographyPolygon),
            typeof (GeographyCollection),
            typeof (GeographyMultiPoint),
            typeof (GeographyMultiLineString),
            typeof (GeographyMultiPolygon),
            typeof (Geometry),
            typeof (GeometryPoint),
            typeof (GeometryLineString),
            typeof (GeometryPolygon),
            typeof (GeometryCollection),
            typeof (GeometryMultiPoint),
            typeof (GeometryMultiLineString),
            typeof (GeometryMultiPolygon)
        };

        /// <summary>
        /// Set of ResourceTypes for this instance of the map.
        /// </summary>
        private readonly ResourceType[] _primitiveResourceTypes;

        /// <summary>
        /// Set of ResourceTypes that can be inherted.
        /// </summary>
        private readonly ResourceType[] _inheritablePrimitiveResourceTypes;

        /// <summary>
        /// Mapping between primitive CLR types, EDM type names, and ResourceTypes.
        /// </summary>
        private static PrimitiveResourceTypeMap _primitiveResourceTypeMapping;

        /// <summary>
        /// Reference to internal <see cref="ResourceType" /> constructor
        /// </summary>
        private static readonly ConstructorInfo ResourceTypeConstructor = typeof(ResourceType).GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, new[] { typeof(Type), typeof(ResourceTypeKind), typeof(string), typeof(string) }, null);

        /// <summary>
        /// Mapping between primitive CLR types, EDM type names, and ResourceTypes.
        /// </summary>
        internal static PrimitiveResourceTypeMap TypeMap
        {
            get
            {
                if (_primitiveResourceTypeMapping == null)
                {
                    var primitiveResourceTypeMap = new PrimitiveResourceTypeMap();
                    Interlocked.CompareExchange(ref _primitiveResourceTypeMapping, primitiveResourceTypeMap, null);
                }

                return _primitiveResourceTypeMapping;
            }
        }

        /// <summary>
        /// Returns all ResourceTypes for this type map.
        /// </summary>
        internal ResourceType[] AllPrimitives
        {
            get { return _primitiveResourceTypes; }
        }

        static PrimitiveResourceTypeMap()
        {
            //
        }

        /// <summary>
        /// Creates a new instance of the type map using the set of all primitive types supported by WCF Data Services.
        /// </summary>
        internal PrimitiveResourceTypeMap()
            : this(BuiltInTypesMapping)
        {
            //
        }

        /// <summary>
        /// Creates a new instance of the type map using the specified set of types.
        /// </summary>
        /// <param name="primitiveTypesEdmNameMapping">Primitive CLR type-to-string mapping information to use to build the type map.</param>
        internal PrimitiveResourceTypeMap(KeyValuePair<Type, string>[] primitiveTypesEdmNameMapping)
        {
            int length = primitiveTypesEdmNameMapping.Length;
            _primitiveResourceTypes = new ResourceType[length];
            var list = new List<ResourceType>(InheritablePrimitiveClrTypes.Length);

            for (int index = 0; index < length; ++index)
            {
                Debug.Assert(ResourceTypeConstructor != null);
                string name = primitiveTypesEdmNameMapping[index].Value.Substring("Edm".Length + 1);
                _primitiveResourceTypes[index] = (ResourceType)ResourceTypeConstructor.Invoke(new object[] { primitiveTypesEdmNameMapping[index].Key, ResourceTypeKind.Primitive, "Edm", name });

                if (InheritablePrimitiveClrTypes.Contains(primitiveTypesEdmNameMapping[index].Key))
                {
                    list.Add(_primitiveResourceTypes[index]);
                }
            }

            _inheritablePrimitiveResourceTypes = list.ToArray();
        }

        /// <summary>
        /// Returns the primitive ResourceType for the specified CLR type.
        /// </summary>
        /// <param name="type">CLR type to use for lookup.</param>
        /// <returns>
        /// Primitive ResourceType that maps to <paramref name="type"/> or null if the type is not mapped.
        /// </returns>
        internal ResourceType GetPrimitive(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            ResourceType resourceType1 = _primitiveResourceTypes.FirstOrDefault(rt => rt.InstanceType == type);

            if (resourceType1 == null)
            {
                foreach (ResourceType resourceType2 in _inheritablePrimitiveResourceTypes)
                {
                    if (resourceType2.InstanceType.IsAssignableFrom(type) &&
                        (resourceType1 == null || resourceType1.InstanceType.IsAssignableFrom(resourceType2.InstanceType)))
                    {
                        resourceType1 = resourceType2;
                    }
                }
            }

            return resourceType1;
        }

        /// <summary>
        /// Returns the primitive ResourceType for the specified EDM type name.
        /// </summary>
        /// <param name="fullEdmTypeName">Fully-qualified EDM type name to use for lookup.</param>
        /// <returns>
        /// Primitive ResourceType that maps to <paramref name="fullEdmTypeName"/> or null if the type is not mapped.
        /// </returns>
        internal ResourceType GetPrimitive(string fullEdmTypeName)
        {
            return _primitiveResourceTypes.FirstOrDefault(rt => rt.FullName == fullEdmTypeName);
        }

        /// <summary>
        /// Whether or not the specified CLR type maps to a primitive ResourceType.
        /// </summary>
        /// <param name="type">CLR type to use for lookup</param>
        /// <returns>
        /// True if <paramref name="type"/> maps to a primitive ResourceType, otherwise false.
        /// </returns>
        internal bool IsPrimitive(Type type)
        {
            return GetPrimitive(type) != null;
        }
    }
}
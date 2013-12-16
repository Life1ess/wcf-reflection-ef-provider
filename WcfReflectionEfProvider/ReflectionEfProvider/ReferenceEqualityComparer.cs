using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace ReflectionEfProvider
{
    /// <summary>
    /// Equality comparer implementation that uses reference equality.
    /// </summary>
    internal class ReferenceEqualityComparer : IEqualityComparer
    {
        /// <summary>
        /// Initializes a new <see cref="T:System.Data.Services.ReferenceEqualityComparer"/> instance.
        /// </summary>
        protected ReferenceEqualityComparer()
        {
            //
        }

        bool IEqualityComparer.Equals(object x, object y)
        {
            return ReferenceEquals(x, y);
        }

        int IEqualityComparer.GetHashCode(object obj)
        {
            return obj != null ? obj.GetHashCode() : 0;
        }
    }

    /// <summary>
    /// Use this class to compare objects by reference in collections such as
    ///             dictionary or hashsets.
    /// 
    /// </summary>
    /// <typeparam name="T">Type of objects to compare.</typeparam>
    /// <remarks>
    /// Typically accesses statically as eg
    ///             ReferenceEqualityComparer&lt;Expression&gt;.Instance.
    /// 
    /// </remarks>
    internal sealed class ReferenceEqualityComparer<T> : ReferenceEqualityComparer, IEqualityComparer<T>
        where T : class
    {
        /// <summary>
        /// Single instance per 'T' for comparison.
        /// </summary>
        private static ReferenceEqualityComparer<T> _instance;

        /// <summary>
        /// Returns a singleton instance for this comparer type.
        /// </summary>
        internal static ReferenceEqualityComparer<T> Instance
        {
            get
            {
                if (_instance == null)
                {
                    var equalityComparer = new ReferenceEqualityComparer<T>();
                    Interlocked.CompareExchange(ref _instance, equalityComparer, null);
                }

                return _instance;
            }
        }

        /// <summary>
        /// Initializes a new <see cref="T:System.Data.Services.ReferenceEqualityComparer"/> instance.
        /// </summary>
        private ReferenceEqualityComparer()
        {
            //
        }

        /// <summary>
        /// Determines whether two objects are the same.
        /// </summary>
        /// <param name="x">First object to compare.</param><param name="y">Second object to compare.</param>
        /// <returns>
        /// true if both are the same; false otherwise.
        /// </returns>
        public bool Equals(T x, T y)
        {
            return ReferenceEquals(x, y);
        }

        /// <summary>
        /// Serves as hashing function for collections.
        /// </summary>
        /// <param name="obj">Object to hash.</param>
        /// <returns>
        /// Hash code for the object; shouldn't change through the lifetime
        ///             of <paramref name="obj"/>.
        /// 
        /// </returns>
        public int GetHashCode(T obj)
        {
            if (obj != null)
            {
                return obj.GetHashCode();
            }
            
            return 0;
        }
    }
}
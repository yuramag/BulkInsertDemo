using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

/*
 * This version of ObjectDataReader is inherited from DbDataReader, but not IDataReader, 
 * which is particularly useful in .NET Core, since it does not define IDataReader interface.
 */

namespace BulkInsertDemo.API.vNext
{
    public sealed class ObjectDataReader<TData> : DbDataReader
    {
        private class PropertyAccessor
        {
            public List<Func<TData, object>> Accessors { get; set; }
            public Dictionary<string, int> Lookup { get; set; }
        }

        private static readonly Lazy<PropertyAccessor> s_propertyAccessorCache = new Lazy<PropertyAccessor>(() =>
        {
            var propertyAccessors = typeof(TData)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(p => p.CanRead)
                .Select((p, i) => new
                {
                    Index = i,
                    Property = p,
                    Accessor = CreatePropertyAccessor(p)
                })
                .ToArray();

            return new PropertyAccessor
            {
                Accessors = propertyAccessors.Select(p => p.Accessor).ToList(),
                Lookup = propertyAccessors.ToDictionary(p => p.Property.Name, p => p.Index, StringComparer.OrdinalIgnoreCase)
            };
        });

        private static Func<TData, object> CreatePropertyAccessor(PropertyInfo p)
        {
            var parameter = Expression.Parameter(typeof(TData), "input");
            var propertyAccess = Expression.Property(parameter, p.GetGetMethod());
            var castAsObject = Expression.TypeAs(propertyAccess, typeof(object));
            var lamda = Expression.Lambda<Func<TData, object>>(castAsObject, parameter);
            return lamda.Compile();
        }

        private IEnumerator<TData> m_dataEnumerator;

        public ObjectDataReader(IEnumerable<TData> data)
        {
            m_dataEnumerator = data.GetEnumerator();
        }

        #region IDataReader Members

        public override int Depth => 1;

        public override bool IsClosed => m_dataEnumerator == null;

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            if (IsClosed)
                throw new ObjectDisposedException(GetType().Name);
            return m_dataEnumerator.MoveNext();
        }

        public override int RecordsAffected => -1;

        public override IEnumerator GetEnumerator()
        {
            return m_dataEnumerator;
        }

        public override bool HasRows { get; } = true;

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (m_dataEnumerator != null)
                {
                    m_dataEnumerator.Dispose();
                    m_dataEnumerator = null;
                }
            }
        }

        #endregion

        #region IDataRecord Members

        public override int GetOrdinal(string name)
        {
            int ordinal;
            if (!s_propertyAccessorCache.Value.Lookup.TryGetValue(name, out ordinal))
                throw new InvalidOperationException("Unknown parameter name: " + name);
            return ordinal;
        }

        public override object GetValue(int i)
        {
            if (m_dataEnumerator == null)
                throw new ObjectDisposedException(GetType().Name);
            return s_propertyAccessorCache.Value.Accessors[i](m_dataEnumerator.Current);
        }

        public override int FieldCount => s_propertyAccessorCache.Value.Accessors.Count;

        #region Not Implemented Members

        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }

        public override object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        #endregion

        #endregion
    }
}
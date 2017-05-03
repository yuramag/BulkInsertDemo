using System.Collections.Generic;

namespace BulkInsertDemo.Helpers
{
    public static class HelperExtensions
    {
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key)
        {
            TValue result;
            return dictionary.TryGetValue(key, out result) ? result : default(TValue);
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue defaultValue)
        {
            TValue result;
            return dictionary.TryGetValue(key, out result) ? result : defaultValue;
        }

        public static T GetValueOrDefault<T>(this IList<T> list, int index)
        {
            return index >= 0 && index < list.Count ? list[index] : default(T);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using BulkInsertDemo.Model;

namespace BulkInsertDemo.Helpers
{
    internal sealed class RandomDataGenerator
    {
        const string c_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        private readonly DateTime m_today = DateTime.Today;
        private readonly Random m_random = new Random();

        private string GetRandomString(int length)
        {
            return new string(Enumerable.Repeat(c_chars, length).Select(s => s[m_random.Next(s.Length)]).ToArray());
        }

        public IEnumerable<Contact> GetContacts(int count)
        {
            return Enumerable.Range(1, count).Select(x => new Contact
            {
                Id = x,
                FirstName = GetRandomString(m_random.Next(5, 25)),
                LastName = GetRandomString(m_random.Next(5, 25)),
                BirthDate = m_today.AddDays(m_random.Next(-20000, 0))
            });
        }

        public IEnumerable<IDictionary<string, object>> GetDynamicData(int count)
        {
            return Enumerable.Range(1, count).Select(GetDynamicRecord);
        }

        private IDictionary<string, object> GetDynamicRecord(int id)
        {
            var result = new Dictionary<string, object>();

            result["Id"] = id;

            AssignValues(result, x => $"I_COL_{x:D2}", () => m_random.Next());
            AssignValues(result, x => $"S_COL_{x:D2}", () => GetRandomString(m_random.Next(5, 25)));
            AssignValues(result, x => $"D_COL_{x:D2}", () => m_today.AddDays(m_random.Next(-20000, 0)));
            AssignValues(result, x => $"G_COL_{x:D2}", () => Guid.NewGuid());

            return result;
        }

        private void AssignValues(IDictionary<string, object> record, Func<int, string> key, Func<object> value)
        {
            foreach (var ind in Enumerable.Range(1, 10).Where(x => m_random.Next(100)%10 != 0))
                record[key(ind)] = value();
        }
    }
}
<h1>Using SqlBulkCopy with IDataReader for Hi Perf Inserts</h1>

<h2>Introduction</h2>

<p>Eventually, <code>SqlBulkCopy</code> can be used in three flavors: inserting data represented as a <code>DataTable</code> object, array of <code>DataRow</code> objects, or <code>IDataReader</code> instance. In this article, I will demonstrate two implementations of <code>IDataReader</code> interface used in conjunction with <code>SqlBulkCopy</code> for high performance database inserts. The other two options are similar to each other and can be used for relatively small amounts of data because they require all records to be pre-loaded into memory before handing them over to <code>SqlBulkCopy</code>. In contrast, the <code>IDataReader</code> approach is more flexible and allows working with unlimited number of records in &quot;lazy&quot; mode, meaning that data can be fed to the <code>SqlBulkCopy</code> on the fly as fast as a server can consume it. This is analogous to <code>IList&lt;T&gt;</code> vs <code>IEnumerable&lt;T&gt;</code> approach.</p>

<h2>Using the Demo</h2>

<p>The attached demo project consists of a pre-compiled console application with config file and <code>Data</code> sub-folder containing sample <code>CSV</code> file. Before running the demo, make sure to adjust config file specifying correct connection <code>string</code> named &quot;<code>DefaultDb</code>&quot;. Another setting &quot;<code>MaxRecordCount</code>&quot; is equal to <code>100,000</code> by default, which should be OK for this demo. Note that the connection <code>string</code> can point to any existing database. All demo tables will be created automatically, so there is no need to set up the database manually.</p>

<p>After launching the demo, it will show up in a console window asking to press <code>Enter</code> before initializing the database and before executing every demo action.</p>

<p>As a first step, the application will attempt to initialize the database. It will create (or recreate) three tables - one for each demo action:</p>

<ol>
	<li><code>Contacts</code> table with <code>Id</code>, <code>FirstName</code>, <code>LastName</code>, and <code>BirthDate</code> columns</li>
	<li><code>DynamicData</code> table with an <code>Id</code>, 10 <code>integer</code>, 10 <code>string</code>, 10 <code>datetime</code>, and 10 <code>guid</code> columns</li>
	<li><code>CsvData</code> table having the same structure as <code>DynamicData</code></li>
</ol>

<p>Then the app will execute three demo actions measuring time for each action:</p>

<ol>
	<li><code>Static Dataset Demo</code> demonstrates <code>ObjectDataReader&lt;T&gt;</code> that allows to process instances of any <code>POCO</code> class (<code>Contact</code> class in this case).</li>
	<li><code>Dynamic Dataset Demo</code> demonstrates <code>DynamicDataReader&lt;T&gt;</code> that also implements <code>IDataReader</code>, but allows user to decide how to extract data from the underlying object of <code>T</code> through the user defined lambda expression. In this demo, I use <code>IDictionary&lt;string, object&gt;</code> to represent the data.</li>
	<li><code>CSV Import Demo</code> utilizes <code>CsvParser</code> class and above-mentioned <code>DynamicDataReader&lt;T&gt;</code> to efficiently load attached &quot;<em>Data\CsvData.csv</em>&quot; file into the database.</li>
</ol>

<div class="callout"><code>CsvParser</code> implementation is described in one of my other articles <a class="external" href="http://www.codeproject.com/Tips/823670/Csharp-Light-and-Fast-CSV-Parser">here</a>.</div>

<p>The data for the first two demos is randomly generated on the fly using helper class <code>RandomDataGenerator</code>. Another helper class <code>TableSchemaProvider</code> is used to extract some metadata from SQL Server and execute some utility SQL commands.</p>

<h2>ObjectDataReader&lt;T&gt;</h2>

<p>As shown below, <code>ObjectDataReader&lt;T&gt;</code> accepts <code>IEnumerable&lt;T&gt;</code> in its constructor, which represents the stream of actual data to be consumed by <code>SqlBulkCopy</code> class. It is important to note that <code>GetOrdinal()</code> and <code>GetValue()</code> methods do not use reflection every time they need to access properties of <code>T</code>. Instead, they use pre-compiled and cached lambda expressions that play the role of property accessors and lookups. These pre-compiled lambda expressions are many times faster than using reflection.</p>

<pre lang="cs">
public sealed class ObjectDataReader&lt;TData&gt; : IDataReader
{
    private class PropertyAccessor
    {
        public List&lt;Func&lt;TData, object&gt;&gt; Accessors { get; set; }
        public Dictionary&lt;string, int&gt; Lookup { get; set; }
    }

    private static readonly Lazy&lt;PropertyAccessor&gt; s_propertyAccessorCache =
        new Lazy&lt;PropertyAccessor&gt;(() =&gt;
    {
        var propertyAccessors = typeof(TData)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p =&gt; p.CanRead)
            .Select((p, i) =&gt; new
            {
                Index = i,
                Property = p,
                Accessor = CreatePropertyAccessor(p)
            })
            .ToArray();

        return new PropertyAccessor
        {
            Accessors = propertyAccessors.Select(p =&gt; p.Accessor).ToList(),
            Lookup = propertyAccessors.ToDictionary(
                p =&gt; p.Property.Name, p =&gt; p.Index, StringComparer.OrdinalIgnoreCase)
        };
    });

    private static Func&lt;TData, object&gt; CreatePropertyAccessor(PropertyInfo p)
    {
        var parameter = Expression.Parameter(typeof(TData), &quot;input&quot;);
        var propertyAccess = Expression.Property(parameter, p.GetGetMethod());
        var castAsObject = Expression.TypeAs(propertyAccess, typeof(object));
        var lamda = Expression.Lambda&lt;Func&lt;TData, object&gt;&gt;(castAsObject, parameter);
        return lamda.Compile();
    }

    private IEnumerator&lt;TData&gt; m_dataEnumerator;

    public ObjectDataReader(IEnumerable&lt;TData&gt; data)
    {
        m_dataEnumerator = data.GetEnumerator();
    }

    #region IDataReader Members

    public void Close()
    {
        Dispose();
    }

    public int Depth =&gt; 1;

    public DataTable GetSchemaTable()
    {
        return null;
    }

    public bool IsClosed =&gt; m_dataEnumerator == null;

    public bool NextResult()
    {
        return false;
    }

    public bool Read()
    {
        if (IsClosed)
            throw new ObjectDisposedException(GetType().Name);
        return m_dataEnumerator.MoveNext();
    }

    public int RecordsAffected =&gt; -1;

    #endregion

    // IDisposable Members

    #region IDataRecord Members

    public int GetOrdinal(string name)
    {
        int ordinal;
        if (!s_propertyAccessorCache.Value.Lookup.TryGetValue(name, out ordinal))
            throw new InvalidOperationException(&quot;Unknown parameter name: &quot; + name);
        return ordinal;
    }

    public object GetValue(int i)
    {
        if (m_dataEnumerator == null)
            throw new ObjectDisposedException(GetType().Name);
        return s_propertyAccessorCache.Value.Accessors[i](m_dataEnumerator.Current);
    }

    public int FieldCount =&gt; s_propertyAccessorCache.Value.Accessors.Count;

    // Not Implemented IDataRecord Members ...
        
    #endregion
}</pre>

<p>Once we have <code>ObjectDataReader&lt;T&gt;</code> implemented, we can plug it into <code>SqlBulkCopy</code> as follows:</p>

<pre lang="cs">
private static async Task RunStaticDatasetDemoAsync(SqlConnection connection, int count, 
    CancellationToken cancellationToken)
{
    using (var bulkCopy = new SqlBulkCopy(connection))
    {
        bulkCopy.DestinationTableName = &quot;Contacts&quot;;
        bulkCopy.BatchSize = 1000;
        bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

        bulkCopy.ColumnMappings.Add(&quot;Id&quot;, &quot;Id&quot;);
        bulkCopy.ColumnMappings.Add(&quot;FirstName&quot;, &quot;FirstName&quot;);
        bulkCopy.ColumnMappings.Add(&quot;LastName&quot;, &quot;LastName&quot;);
        bulkCopy.ColumnMappings.Add(&quot;BirthDate&quot;, &quot;BirthDate&quot;);

        using (var reader = new ObjectDataReader&lt;Contact&gt;(new RandomDataGenerator().GetContacts(count)))
            await bulkCopy.WriteToServerAsync(reader, cancellationToken);
    }
}</pre>

<h2>DynamicDataReader&lt;T&gt;</h2>

<p>You can use <code>DynamicDataReader&lt;T&gt;</code> if there is no statically defined class that represents the data. The best example that illustrates the purpose of <code>DynamicDataReader&lt;T&gt;</code> is when every record of your table is represented as <code>Dictionary&lt;string, object&gt;</code> where the keys are column names. This way, if there is no value for a given column in the dictionary, <code>Null</code> value will be assumed. Conversely, all items in the dictionary that are not associated with any column in the table, will be ignored.</p>

<pre lang="cs">
public sealed class DynamicDataReader&lt;T&gt; : IDataReader
{
    private readonly IList&lt;SchemaFieldDef&gt; m_schema;
    private readonly IDictionary&lt;string, int&gt; m_schemaMapping;
    private readonly Func&lt;T, string, object&gt; m_selector;
    private IEnumerator&lt;T&gt; m_dataEnumerator;

    public DynamicDataReader(IList&lt;SchemaFieldDef&gt; schema, IEnumerable&lt;T&gt; data, 
        Func&lt;T, string, object&gt; selector)
    {
        m_schema = schema;
        m_schemaMapping = m_schema
            .Select((x, i) =&gt; new { x.FieldName, Index = i })
            .ToDictionary(x =&gt; x.FieldName, x =&gt; x.Index);
        m_selector = selector;
        m_dataEnumerator = data.GetEnumerator();
    }

    #region IDataReader Members

    public void Close()
    {
        Dispose();
    }

    public int Depth =&gt; 1;

    public DataTable GetSchemaTable()
    {
        return null;
    }

    public bool IsClosed =&gt; m_dataEnumerator == null;

    public bool NextResult()
    {
        return false;
    }

    public bool Read()
    {
        if (IsClosed)
            throw new ObjectDisposedException(GetType().Name);
        return m_dataEnumerator.MoveNext();
    }

    public int RecordsAffected =&gt; -1;

    #endregion

    // IDisposable Members

    #region IDataRecord Members

    public int FieldCount =&gt; m_schema.Count;

    public int GetOrdinal(string name)
    {
        int ordinal;
        if (!m_schemaMapping.TryGetValue(name, out ordinal))
            throw new InvalidOperationException(&quot;Unknown parameter name: &quot; + name);
        return ordinal;
    }

    public object GetValue(int i)
    {
        if (m_dataEnumerator == null)
            throw new ObjectDisposedException(GetType().Name);

        var value = m_selector(m_dataEnumerator.Current, m_schema[i].FieldName);

        if (value == null)
            return DBNull.Value;

        var strValue = value as string;
        if (strValue != null)
        {
            if (strValue.Length &gt; m_schema[i].Size &amp;&amp; m_schema[i].Size &gt; 0)
                strValue = strValue.Substring(0, m_schema[i].Size);
            if (m_schema[i].DataType == DbType.String)
                return strValue;
            return SchemaFieldDef.StringToTypedValue(strValue, m_schema[i].DataType) ?? DBNull.Value;
        }

        return value;
    }

    // Not Implemented IDataRecord Members

    #endregion
}</pre>

<p><code>DynamicDataReader&lt;T&gt;</code> relays on <code>SchemaFieldDef</code> class that describes Field Name, Size, and DB Data Type of a table column. Only those columns that were passed via constructor (<code>IList&lt;SchemaFieldDef&gt; schema</code>) will participate in data inserts. The other two parameter of a constructor represent the data itself (<code>IEnumerable&lt;T&gt; data</code>), and the user defined lambda expression (<code>Func&lt;T, string, object&gt; selector</code>) to access properties. As you can see, <code>selector</code> accepts the instance of <code>T</code> and a <code>string</code> field name, and returns <code>object</code> back that represents the value associated with that field name. Note that <code>object</code>&#39;s data type can either be a non-<code>string</code> C# type (<code>int</code>, <code>decimal</code>, <code>DateTime</code>, <code>Guid</code>, etc.) corresponding to the actual type in database (<code>int</code>, <code>numeric</code>, <code>datetime</code>, <code>uniqueidentifier</code>, etc.), or simply a <code>string</code>. In latter case, <code>DynamicDataReader</code> will attempt to convert a <code>string</code> value into an appropriate data type automatically, with the help of <code>SchemaFieldDef.StringToTypedValue()</code> method. This method supports only a few data type, but can be easily extended if needed.</p>

<p>Here is an example of using <code>DynamicDataReader&lt;T&gt;</code> together with <code>SqlBulkCopy</code>:</p>

<pre lang="cs">
private static async Task RunDynamicDatasetDemoAsync(SqlConnection connection, int count, 
    CancellationToken cancellationToken)
{
    var fields = await new TableSchemaProvider(connection, &quot;DynamicData&quot;).GetFieldsAsync();

    using (var bulkCopy = new SqlBulkCopy(connection))
    {
        bulkCopy.DestinationTableName = &quot;DynamicData&quot;;
        bulkCopy.BatchSize = 1000;
        bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

        foreach (var field in fields)
            bulkCopy.ColumnMappings.Add(field.FieldName, field.FieldName);

        var data = new RandomDataGenerator().GetDynamicData(count);

        using (var reader = new DynamicDataReader&lt;IDictionary&lt;string, object&gt;&gt;
				(fields, data, (x, k) =&gt; x.GetValueOrDefault(k)))
            await bulkCopy.WriteToServerAsync(reader, cancellationToken);
    }
}</pre>

<p>It is very similar to <code>ObjectDataReader</code> usage with the exception that the fields are not statically bound.</p>

<h2>CSV File Import</h2>

<p>Finally, the third demo action features <code>CsvParser</code>, <code>DynamicDataReader</code>, and <code>SqlBulkCopy</code> classes working together to achieve high performant and scalable data import in <code>CSV</code> format:</p>

<pre lang="cs">
private static async Task RunCsvDatasetDemoAsync(SqlConnection connection, int count, 
    CancellationToken cancellationToken)
{
    using (var csvReader = new StreamReader(@&quot;Data\CsvData.csv&quot;))
    {
        var csvData = CsvParser.ParseHeadAndTail(csvReader, &#39;,&#39;, &#39;&quot;&#39;);

        var csvHeader = csvData.Item1
            .Select((x, i) =&gt; new {Index = i, Field = x})
            .ToDictionary(x =&gt; x.Field, x =&gt; x.Index);

        var csvLines = csvData.Item2;

        var fields = await new TableSchemaProvider(connection, &quot;CsvData&quot;).GetFieldsAsync();

        using (var bulkCopy = new SqlBulkCopy(connection))
        {
            bulkCopy.DestinationTableName = &quot;CsvData&quot;;
            bulkCopy.BatchSize = 1000;
            bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

            foreach (var field in fields)
                bulkCopy.ColumnMappings.Add(field.FieldName, field.FieldName);

            using (var reader = new DynamicDataReader&lt;IList&lt;string&gt;&gt;(fields, csvLines.Take(count),
                (x, k) =&gt; x.GetValueOrDefault(csvHeader.GetValueOrDefault(k, -1))))
            {
                await bulkCopy.WriteToServerAsync(reader, cancellationToken);
            }
        }
    }
}</pre>

<p>For demo purposes, there are only <code>1,000</code> rows in the <em>CsvData.csv</em> file. This particular solution though will be able to handle any number of rows with relatively stable performance. It will match column names in the <code>CSV</code> file with column names of the target table. Missing data will be populated with <code>Null</code>s. Any extra columns not existing in the target table will be ignored.</p>

<h2>Summary</h2>

<p>In this article, I demonstrated one of the possible ways of handling high perfomant database inserts using managed code. My goal was to construct a flexible and easy to use API, so it could be applied to many different scenarios. In particular, use <code>ObjectDataReader&lt;T&gt;</code> to upload data represented as statically defined <code>POCO</code> classes, and <code>DynamicDataReader&lt;T&gt;</code> to upload data of any structure.</p>

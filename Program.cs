using System;
using System.Reflection;
using NUnit.Framework;
using NUnitLite;

class Program
{
    static int Main(string[] args)
    {
        // Create a test suite
        var testSuite = new AutoRun(Assembly.GetExecutingAssembly());

        // Run the tests and return the result
        return testSuite.Execute(args);
    }
}

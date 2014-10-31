using Xunit;
using Xunit.Extensions;

namespace AzurePageBlobStream.Tests
{
    public class WriteOperationTests
    {
        [Theory]
        [InlineData(0, 10, 2, 10, true)]
        [InlineData(2, 10, 0, 3, true)]
        [InlineData(0, 10, 0, 10, true)]
        [InlineData(0, 10, 1, 4, true)]
        [InlineData(1, 4, 0, 10, true)]
        [InlineData(0, 10, 1, 10, true)]
        [InlineData(0, 10, 0, 10, true)]
        [InlineData(0, 10, 10, 10, true)]
        [InlineData(10, 10, 0, 10, true)]
        [InlineData(0, 5, 5, 3, true)]
        [InlineData(5, 3, 0, 5, true)]
        [InlineData(1, 3, 5, 5, false)]
        [InlineData(5, 5, 1, 3, false)]
        public void CanBeMerged(long currentPosition, int currentCount, long toMergePosition, int toMergeCount,
            bool expected)
        {
            var current = new WriteOperation(currentPosition, new byte[currentCount], 0, currentCount);
            var toMerge = new WriteOperation(toMergePosition, new byte[toMergeCount], 0, toMergeCount);
            var actual = current.CanBeMerged(toMerge);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(0, 10, 2, 10,0,12)]
        [InlineData(2, 10, 0, 3,0,12)]
        [InlineData(0, 10, 0, 10, 0,10)]
        [InlineData(0, 10, 1, 4,0,10)]
        [InlineData(1, 4, 0, 10,0,10)]
        [InlineData(0, 10, 1, 10, 0,11)]
        [InlineData(0, 10, 10, 10, 0,20)]
        [InlineData(10, 10, 0, 10,0,20)]
        [InlineData(0, 5, 5, 3, 0,8)]
        [InlineData(5, 3, 0, 5, 0,8)]
        public void Merge(long currentPosition, int currentCount, long toMergePosition, int toMergeCount,
            long mergedPosition, int mergedCount)
        {
            var current = new WriteOperation(currentPosition, new byte[currentCount], 0, currentCount);
            var toMerge = new WriteOperation(toMergePosition, new byte[toMergeCount], 0, toMergeCount);
            current.Merge(toMerge);
            Assert.Equal(mergedPosition, current.Position);
            Assert.Equal(mergedCount, current.Count);
        }
    }
}
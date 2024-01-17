namespace NQueue.Internal
{
    internal class InternalConfig
    {
        public static readonly InternalConfig AsEnabled = new() { Enabled = true };
        public static readonly InternalConfig AsDisabled = new() { Enabled = false };
        public static readonly InternalConfig AsDisabledAndUnitTest = new() { Enabled = false, IsUnitTest = true};
        public bool Enabled { get; private set; }
        public bool IsUnitTest { get; private set; }
    }
}
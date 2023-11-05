namespace NQueue.Internal
{
    internal class InternalConfig
    {
        public static readonly InternalConfig AsEnabled = new() { Enabled = true };
        public static readonly InternalConfig AsDisabled = new() { Enabled = false };
        public bool Enabled { get; private set; }
    }
}
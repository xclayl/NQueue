namespace NQueue.Internal
{
    internal class InternalConfig
    {
        public static readonly InternalConfig AsEnabled = new InternalConfig() { Enabled = true };
        public static readonly InternalConfig AsDisabled = new InternalConfig() { Enabled = false };
        public bool Enabled { get; private set; }
    }
}
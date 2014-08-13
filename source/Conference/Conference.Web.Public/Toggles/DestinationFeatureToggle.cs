namespace Conference.Web.Public.Toggles
{
    using FeatureSwitch;
    using FeatureSwitch.Strategies;

    [AppSettings(Key = "Toggles.DestinationFeatureToggle")]
    public class DestinationFeatureToggle : BaseFeature
    {
    }
}
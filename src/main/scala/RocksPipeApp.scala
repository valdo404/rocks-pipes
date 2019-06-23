import cats.effect.ExitCode

trait RocksPipeApp[F[_]] {
  def useDatabase: F[ExitCode]
}

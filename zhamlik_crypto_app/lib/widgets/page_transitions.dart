import 'package:flutter/material.dart';
import 'package:animations/animations.dart';

class CustomPageTransitions {
  /// Shared axis transition - for navigational flow
  static Widget sharedAxisTransition({
    required BuildContext context,
    required Widget child,
    required GlobalKey<NavigatorState> navigatorKey,
  }) {
    return PageTransitionSwitcher(
      duration: const Duration(milliseconds: 300),
      transitionBuilder: (child, animation, secondaryAnimation) {
        return SharedAxisTransition(
          animation: animation,
          secondaryAnimation: secondaryAnimation,
          transitionType: SharedAxisTransitionType.horizontal,
          child: child,
        );
      },
      child: child,
    );
  }

  /// Fade through transition - for related content
  static Widget fadeThroughTransition({
    required BuildContext context,
    required Widget child,
  }) {
    return PageTransitionSwitcher(
      duration: const Duration(milliseconds: 300),
      transitionBuilder: (child, animation, secondaryAnimation) {
        return FadeThroughTransition(
          animation: animation,
          secondaryAnimation: secondaryAnimation,
          child: child,
        );
      },
      child: child,
    );
  }

  /// Fade scale transition - for expanding content
  static Widget fadeScaleTransition({
    required BuildContext context,
    required Widget child,
  }) {
    return PageTransitionSwitcher(
      duration: const Duration(milliseconds: 300),
      transitionBuilder: (child, animation, secondaryAnimation) {
        return FadeScaleTransition(
          animation: animation,
          child: child,
        );
      },
      child: child,
    );
  }
}

/// Custom Route with Shared Axis Transition
class SharedAxisPageRoute extends PageRouteBuilder {
  final Widget child;

  SharedAxisPageRoute({required this.child})
      : super(
          pageBuilder: (context, animation, secondaryAnimation) => child,
          transitionDuration: const Duration(milliseconds: 300),
          reverseTransitionDuration: const Duration(milliseconds: 300),
          transitionsBuilder: (context, animation, secondaryAnimation, child) {
            return SharedAxisTransition(
              animation: animation,
              secondaryAnimation: secondaryAnimation,
              transitionType: SharedAxisTransitionType.horizontal,
              child: child,
            );
          },
        );
}

/// Custom Route with Fade Through Transition
class FadeThroughPageRoute extends PageRouteBuilder {
  final Widget child;

  FadeThroughPageRoute({required this.child})
      : super(
          pageBuilder: (context, animation, secondaryAnimation) => child,
          transitionDuration: const Duration(milliseconds: 300),
          reverseTransitionDuration: const Duration(milliseconds: 300),
          transitionsBuilder: (context, animation, secondaryAnimation, child) {
            return FadeThroughTransition(
              animation: animation,
              secondaryAnimation: secondaryAnimation,
              child: child,
            );
          },
        );
}

/// Custom Route with Fade Scale Transition
class FadeScalePageRoute extends PageRouteBuilder {
  final Widget child;

  FadeScalePageRoute({required this.child})
      : super(
          pageBuilder: (context, animation, secondaryAnimation) => child,
          transitionDuration: const Duration(milliseconds: 300),
          reverseTransitionDuration: const Duration(milliseconds: 300),
          transitionsBuilder: (context, animation, secondaryAnimation, child) {
            return FadeScaleTransition(
              animation: animation,
              child: child,
            );
          },
        );
}

/// Slide Transition Builder
class SlidePageRoute extends PageRouteBuilder {
  final Widget child;
  final Offset beginOffset;
  final Offset endOffset;

  SlidePageRoute({
    required this.child,
    this.beginOffset = const Offset(1.0, 0.0),
    this.endOffset = Offset.zero,
  }) : super(
          pageBuilder: (context, animation, secondaryAnimation) => child,
          transitionDuration: const Duration(milliseconds: 300),
          reverseTransitionDuration: const Duration(milliseconds: 300),
          transitionsBuilder: (context, animation, secondaryAnimation, child) {
            final tween = Tween(begin: beginOffset, end: endOffset);
            final curvedAnimation = CurvedAnimation(
              parent: animation,
              curve: Curves.easeInOutCubic,
            );
            return SlideTransition(
              position: tween.animate(curvedAnimation),
              child: child,
            );
          },
        );
}

/// Scale Transition Builder
class ScalePageRoute extends PageRouteBuilder {
  final Widget child;

  ScalePageRoute({required this.child})
      : super(
          pageBuilder: (context, animation, secondaryAnimation) => child,
          transitionDuration: const Duration(milliseconds: 300),
          reverseTransitionDuration: const Duration(milliseconds: 300),
          transitionsBuilder: (context, animation, secondaryAnimation, child) {
            final tween = Tween<double>(begin: 0.8, end: 1.0);
            final curvedAnimation = CurvedAnimation(
              parent: animation,
              curve: Curves.easeOutBack,
            );
            return ScaleTransition(
              scale: tween.animate(curvedAnimation),
              child: child,
            );
          },
        );
}

/// Helper class for navigation with animations
class NavigationHelper {
  /// Navigate with shared axis transition
  static void navigateSharedAxis(
    BuildContext context,
    String routeName, {
    Object? arguments,
  }) {
    Navigator.of(context).push(
      SharedAxisPageRoute(
        child: _buildRouteWidget(routeName, arguments),
      ),
    );
  }

  /// Navigate with fade through transition
  static void navigateFadeThrough(
    BuildContext context,
    String routeName, {
    Object? arguments,
  }) {
    Navigator.of(context).push(
      FadeThroughPageRoute(
        child: _buildRouteWidget(routeName, arguments),
      ),
    );
  }

  /// Navigate with fade scale transition
  static void navigateFadeScale(
    BuildContext context,
    String routeName, {
    Object? arguments,
  }) {
    Navigator.of(context).push(
      FadeScalePageRoute(
        child: _buildRouteWidget(routeName, arguments),
      ),
    );
  }

  /// Navigate with slide transition
  static void navigateSlide(
    BuildContext context,
    String routeName, {
    Object? arguments,
  }) {
    Navigator.of(context).push(
      SlidePageRoute(
        child: _buildRouteWidget(routeName, arguments),
      ),
    );
  }

  /// Navigate with scale transition
  static void navigateScale(
    BuildContext context,
    String routeName, {
    Object? arguments,
  }) {
    Navigator.of(context).push(
      ScalePageRoute(
        child: _buildRouteWidget(routeName, arguments),
      ),
    );
  }

  static Widget _buildRouteWidget(String routeName, Object? arguments) {
    // This would normally use the route lookup from MaterialApp
    // For now, return a placeholder
    return Placeholder();
  }
}

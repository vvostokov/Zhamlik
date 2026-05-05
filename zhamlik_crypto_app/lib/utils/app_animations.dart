import 'package:flutter/material.dart';
import 'package:animations/animations.dart';

/// Utility class for app animations
class AppAnimations {
  // Page transitions
  static const pageTransitionDuration = Duration(milliseconds: 300);

  static Widget sharedAxisTransition({
    required Widget child,
    required MaterialPageRoute route,
  }) {
    return WidgetUtils.showTransition(
      child: child,
      transitionBuilder: (child, animation) {
        return SharedAxisTransition(
          animation: animation,
          transitionType: SharedAxisTransitionType.horizontal,
          child: child,
        );
      },
    );
  }

  // Fade through transition
  static Widget fadeThroughTransition({
    required Widget child,
    required Animation<double> animation,
  }) {
    return FadeThroughTransition(
      animation: animation,
      child: child,
    );
  }

  // Scale transition
  static Widget scaleTransition({
    required Widget child,
    required Animation<double> animation,
  }) {
    return ScaleTransition(
      scale: CurvedAnimation(
        parent: animation,
        curve: Curves.easeOutBack,
      ).drive(Tween<double>(begin: 0.8, end: 1.0)),
      child: child,
    );
  }

  // Fade transition
  static Widget fadeTransition({
    required Widget child,
    required Animation<double> animation,
  }) {
    return FadeTransition(
      opacity: CurvedAnimation(
        parent: animation,
        curve: Curves.easeInOut,
      ),
      child: child,
    );
  }

  // Slide transition
  static Widget slideTransition({
    required Widget child,
    required Animation<double> animation,
    Offset begin = const Offset(0.0, 0.1),
  }) {
    return SlideTransition(
      position: Tween<Offset>(
        begin: begin,
        end: Offset.zero,
      ).animate(CurvedAnimation(
        parent: animation,
        curve: Curves.easeOutCubic,
      )),
      child: child,
    );
  }

  // Staggered animation for list items
  static Widget staggeredAnimation({
    required Widget child,
    required Animation<double> animation,
    required int index,
    double delayFactor = 0.1,
  }) {
    return FadeTransition(
      opacity: CurvedAnimation(
        parent: animation,
        curve: Interval(
          index * delayFactor,
          0.6 + (index * delayFactor),
          curve: Curves.easeOut,
        ),
      ),
      child: SlideTransition(
        position: Tween<Offset>(
          begin: const Offset(0, 0.1),
          end: Offset.zero,
        ).animate(CurvedAnimation(
          parent: animation,
          curve: Interval(
            index * delayFactor,
            0.6 + (index * delayFactor),
            curve: Curves.easeOut,
          ),
        )),
        child: child,
      ),
    );
  }

  // Shrink/expand animation
  static Widget shrinkExpand({
    required Widget child,
    required Animation<double> animation,
  }) {
    return ClipRect(
      child: Align(
        heightFactor: CurvedAnimation(
          parent: animation,
          curve: Curves.easeInOut,
        ).drive(Tween<double>(begin: 0.0, end: 1.0)),
        child: child,
      ),
    );
  }
}

/// Custom page route with animation
class AnimatedPageRoute extends MaterialPageRoute {
  final String transitionType;

  AnimatedPageRoute({
    required WidgetBuilder builder,
    this.transitionType = 'fade',
    super.settings,
    super.fullscreenDialog,
  }) : super(builder: builder);

  @override
  Widget buildTransitions<T>(
    PageRoute<T> route,
    BuildContext context,
    Animation<double> animation,
    Animation<double> secondaryAnimation,
    Widget child,
  ) {
    switch (transitionType) {
      case 'sharedAxis':
        return SharedAxisTransition(
          animation: animation,
          transitionType: SharedAxisTransitionType.horizontal,
          child: child,
        );
      case 'fadeThrough':
        return FadeThroughTransition(
          animation: animation,
          child: child,
        );
      default:
        return FadeTransition(
          opacity: CurvedAnimation(
            parent: animation,
            curve: Curves.easeInOut,
          ),
          child: child,
        );
    }
  }
}

/// Animated card with hover effect
class AnimatedCard extends StatefulWidget {
  final Widget child;
  final VoidCallback? onTap;
  final Color? color;
  final double? elevation;
  final BorderRadius? borderRadius;
  final Duration duration;

  const AnimatedCard({
    super.key,
    required this.child,
    this.onTap,
    this.color,
    this.elevation,
    this.borderRadius,
    this.duration = const Duration(milliseconds: 200),
  });

  @override
  State<AnimatedCard> createState() => _AnimatedCardState();
}

class _AnimatedCardState extends State<AnimatedCard> {
  bool _isPressed = false;

  @override
  Widget build(BuildContext context) {
    return AnimatedContainer(
      duration: widget.duration,
      curve: Curves.easeInOut,
      decoration: BoxDecoration(
        color: widget.color ?? Theme.of(context).cardColor,
        borderRadius: widget.borderRadius ?? BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: (_isPressed
                    ? Theme.of(context).shadowColor.withOpacity(0.1)
                    : Theme.of(context).shadowColor.withOpacity(0.05)),
            blurRadius: _isPressed ? 4 : 8,
            offset: _isPressed ? const Offset(0, 2) : const Offset(0, 4),
          ),
        ],
      ),
      transform: Matrix4.identity()..scale(_isPressed ? 0.98 : 1.0),
      child: Material(
        color: Colors.transparent,
        child: InkWell(
          borderRadius: widget.borderRadius ?? BorderRadius.circular(16),
          onTap: widget.onTap,
          onTapDown: (_) => setState(() => _isPressed = true),
          onTapUp: (_) => setState(() => _isPressed = false),
          onTapCancel: () => setState(() => _isPressed = false),
          child: widget.child,
        ),
      ),
    );
  }
}

/// Pulsing icon for refresh/sync indicators
class PulsingIcon extends StatefulWidget {
  final IconData icon;
  final Color? color;
  final double size;

  const PulsingIcon({
    super.key,
    required this.icon,
    this.color,
    this.size = 24,
  });

  @override
  State<PulsingIcon> createState() => _PulsingIconState();
}

class _PulsingIconState extends State<PulsingIcon>
    with SingleTickerProviderStateMixin {
  late AnimationController _controller;
  late Animation<double> _animation;

  @override
  void initState() {
    super.initState();
    _controller = AnimationController(
      duration: const Duration(seconds: 1),
      vsync: this,
    );
    _animation = Tween<double>(begin: 0.5, end: 1.0).animate(
      CurvedAnimation(parent: _controller, curve: Curves.easeInOut),
    );
    _controller.repeat(reverse: true);
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: _animation,
      builder: (context, child) {
        return Opacity(
          opacity: _animation.value,
          child: Icon(
            widget.icon,
            color: widget.color,
            size: widget.size,
          ),
        );
      },
    );
  }
}

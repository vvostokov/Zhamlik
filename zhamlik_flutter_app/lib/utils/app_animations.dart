import 'package:flutter/material.dart';
import 'package:animations/animations.dart';

/// Utility class for app animations (Banking)
class AppAnimations {
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
